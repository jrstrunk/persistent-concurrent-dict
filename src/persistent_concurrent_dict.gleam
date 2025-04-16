/// A concurrent dictionary, fully backed by a SQLite database on disk.
import concurrent_dict
import filepath
import gleam/dynamic/decode
import gleam/erlang/process
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

fn handle_persist_data(msg, state: ConnectionActorState(key, val)) {
  case msg {
    PersistData(key, val, reply) -> {
      let encoded_key = state.key_encoder(key)
      let encoded_val = state.val_encoder(val)

      let res =
        insert_query(encoded_key, encoded_val)
        |> sqlight.exec(on: state.conn)
        |> snag.map_error(string.inspect)
        |> snag.context("Unable to insert data")

      process.send(reply, res)
    }
  }

  actor.continue(state)
}

pub opaque type PersistentConcurrentDict(key, val) {
  PersistentConcurrentDict(
    conn_actor: process.Subject(ConnectionActorMsg(key, val)),
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
  })

  use conn <- result.try(
    sqlight.open(path)
    |> snag.map_error(string.inspect),
  )

  use Nil <- result.try(
    sqlight.exec(table_schema, on: conn)
    |> snag.map_error(string.inspect),
  )

  use data_list <- result.try(
    sqlight.query(select_query, with: [], on: conn, expecting: {
      use key <- decode.field(0, decode.string)
      use value <- decode.field(1, decode.string)
      decode.success(#(key_decoder(key), val_decoder(value)))
    })
    |> snag.map_error(string.inspect),
  )

  let data = concurrent_dict.from_list(data_list)

  use conn_actor <- result.map(
    actor.start(
      ConnectionActorState(conn:, key_encoder:, val_encoder:),
      handle_persist_data,
    )
    |> snag.map_error(string.inspect),
  )

  PersistentConcurrentDict(conn_actor:, data:)
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

pub fn insert(pcd: PersistentConcurrentDict(key, val), key, val) {
  // First persist the data in the disk database
  use Nil <- result.map(
    process.try_call(pcd.conn_actor, PersistData(key, val, _), 100_000)
    |> snag.map_error(string.inspect)
    |> result.flatten,
  )

  // If that succeeds, then add it to the in-memory store
  concurrent_dict.insert(pcd.data, key, val)
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
