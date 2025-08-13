/// A concurrent dictionary, fully backed by a SQLite database on disk.
import concurrent_dict
import filepath
import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/list
import gleam/otp/actor
import gleam/result
import gleam/string
import simplifile
import snag
import sqlight

pub type ConnectionActorState(key, val) {
  ConnectionActorState(
    conn: sqlight.Connection,
    key_encoder: fn(key) -> String,
    val_encoder: fn(val) -> String,
  )
}

pub type ConnectionActorMsg(key, val) {
  PersistData(key, val, reply: process.Subject(Result(Nil, snag.Snag)))
}

fn handle_persist_data(state: ConnectionActorState(key, val), msg) {
  case msg {
    PersistData(key, val, reply) -> {
      let encoded_key = state.key_encoder(key)
      let encoded_val = state.val_encoder(val)

      let res =
        insert_query(encoded_key, encoded_val)
        |> sqlight.exec(on: state.conn)
        |> snag.map_error(string.inspect)
        |> snag.context("Unable to insert data into persist database")

      process.send(reply, res)
    }
  }

  actor.continue(state)
}

pub type SubscribersActorState(key) {
  SubscribersActorState(
    subscribers: List(fn() -> Nil),
    key_subscribers: dict.Dict(key, List(fn() -> Nil)),
  )
}

pub type SubscribersActorMsg(key) {
  Subscribe(fn() -> Nil, process.Subject(Nil))
  KeySubscribe(key, fn() -> Nil, process.Subject(Nil))
  NotifySubscribers
  NotifyKeySubscribers(key)
}

fn handle_subscribers(state: SubscribersActorState(key), msg) {
  case msg {
    Subscribe(subscriber, reply) -> {
      let subscribers = [subscriber, ..state.subscribers]

      process.send(reply, Nil)

      actor.continue(SubscribersActorState(..state, subscribers:))
    }
    KeySubscribe(key, subscriber, reply) -> {
      let new_key_subscribers = case dict.get(state.key_subscribers, key) {
        Ok(subscribers) -> [subscriber, ..subscribers]
        Error(Nil) -> [subscriber]
      }

      let key_subscribers =
        dict.insert(state.key_subscribers, key, new_key_subscribers)

      process.send(reply, Nil)

      actor.continue(SubscribersActorState(..state, key_subscribers:))
    }
    NotifySubscribers -> {
      state.subscribers
      |> list.each(fn(subscriber) { subscriber() })
      actor.continue(state)
    }
    NotifyKeySubscribers(key) -> {
      case dict.get(state.key_subscribers, key) {
        Ok(subscribers) -> {
          list.each(subscribers, fn(subscriber) { subscriber() })
        }
        Error(Nil) -> Nil
      }
      actor.continue(state)
    }
  }
}

pub opaque type PersistentConcurrentDict(key, val) {
  PersistentConcurrentDict(
    conn_actor: process.Subject(ConnectionActorMsg(key, val)),
    subscribers_actor: process.Subject(SubscribersActorMsg(key)),
    data: concurrent_dict.ConcurrentDict(key, val),
  )
}

pub fn build(
  persist_path path: String,
  key_encoder key_encoder: fn(key) -> String,
  key_decoder key_decoder: fn(String) -> key,
  val_encoder val_encoder: fn(val) -> String,
  val_decoder val_decoder: fn(String) -> val,
) {
  use Nil <- result.try(case path == ":memory:" {
    True -> Ok(Nil)
    False ->
      filepath.directory_name(path)
      |> simplifile.create_directory_all
      |> snag.map_error(simplifile.describe_error)
      |> snag.context("Failed to create persist directory")
  })

  use conn <- result.try(
    sqlight.open(path)
    |> snag.map_error(string.inspect)
    |> snag.context("Failed to open persist database at " <> path),
  )

  use Nil <- result.try(
    sqlight.exec(table_schema, on: conn)
    |> snag.map_error(string.inspect)
    |> snag.context("Failed to create table schema"),
  )

  use data_list <- result.try(
    sqlight.query(select_query, with: [], on: conn, expecting: {
      use key <- decode.field(0, decode.string)
      use value <- decode.field(1, decode.string)
      decode.success(#(key_decoder(key), val_decoder(value)))
    })
    |> snag.map_error(string.inspect)
    |> snag.context("Failed to select existing persist data from database"),
  )

  let data = concurrent_dict.from_list(data_list)

  use conn_actor <- result.try(
    actor.new(ConnectionActorState(conn:, key_encoder:, val_encoder:))
    |> actor.on_message(handle_persist_data)
    |> actor.start
    |> result.map(fn(actor) { actor.data })
    |> snag.map_error(string.inspect)
    |> snag.context("Failed to start connection actor"),
  )

  use subscribers_actor <- result.map(
    actor.new(SubscribersActorState(
      subscribers: [],
      key_subscribers: dict.new(),
    ))
    |> actor.on_message(handle_subscribers)
    |> actor.start
    |> result.map(fn(actor) { actor.data })
    |> snag.map_error(string.inspect)
    |> snag.context("Failed to start subscribers actor"),
  )

  PersistentConcurrentDict(conn_actor:, data:, subscribers_actor:)
}

const table_schema = "
CREATE TABLE IF NOT EXISTS persist (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
)"

fn insert_query(key, value) {
  "INSERT OR REPLACE INTO persist (key, value) VALUES ('"
  <> key
  <> "', '"
  <> value
  <> "')"
}

const select_query = "SELECT key, value FROM persist"

pub fn subscribe(pcd: PersistentConcurrentDict(key, val), subscriber) {
  process.call(pcd.subscribers_actor, 100_000, Subscribe(subscriber, _))
}

pub fn subscribe_to_key(
  pcd: PersistentConcurrentDict(key, val),
  key,
  subscriber,
) {
  process.call(pcd.subscribers_actor, 100_000, KeySubscribe(key, subscriber, _))
}

pub fn insert(pcd: PersistentConcurrentDict(key, val), key, val) {
  // First persist the data in the disk database
  use Nil <- result.map(
    process.call(pcd.conn_actor, 100_000, PersistData(key, val, _)),
  )

  // If that succeeds, then add it to the in-memory store
  concurrent_dict.insert(pcd.data, key, val)

  // Notify subscribers
  process.send(pcd.subscribers_actor, NotifySubscribers)

  // Notify key subscribers
  process.send(pcd.subscribers_actor, NotifyKeySubscribers(key))
}

pub fn get(pcd: PersistentConcurrentDict(key, val), key) {
  concurrent_dict.get(pcd.data, key)
}

pub fn from_list(pcd: PersistentConcurrentDict(key, val), list) {
  PersistentConcurrentDict(..pcd, data: concurrent_dict.from_list(list))
}

pub fn to_list(pcd: PersistentConcurrentDict(key, val)) {
  concurrent_dict.to_list(pcd.data)
}
