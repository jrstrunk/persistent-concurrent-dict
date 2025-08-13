/// A dictionary stored on disk with a concurrent dictionary acting as an 
/// in-memory cache for recently accessed data. Getting data from the cache
/// is concurrent and fast, but missing the cache will result in an expensive, 
/// non-concurrent disk database read.
import concurrent_dict
import concurrent_dict/counter as concurrent_counter
import filepath
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
    val_decoder: fn(String) -> val,
  )
}

pub type ConnectionActorMsg(key, val) {
  PersistData(key, val, reply: process.Subject(Result(Nil, snag.Snag)))
  GetData(key, reply: process.Subject(Result(val, snag.Snag)))
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
        |> snag.context("Unable to insert data")

      process.send(reply, res)
    }

    GetData(key, reply) -> {
      let encoded_key = state.key_encoder(key)

      let res =
        select_query
        |> sqlight.query(
          with: [sqlight.text(encoded_key)],
          on: state.conn,
          expecting: {
            use value <- decode.field(0, decode.string)
            decode.success(state.val_decoder(value))
          },
        )
        |> snag.map_error(string.inspect)
        |> snag.context("Unable to select data")
        |> result.try(fn(res) {
          list.first(res)
          |> result.replace_error(snag.new("No data found"))
        })

      process.send(reply, res)
    }
  }

  actor.continue(state)
}

pub opaque type CacheLimitedPersistentConcurrentDict(key, val) {
  CacheLimitedPersistentConcurrentDict(
    conn_actor: process.Subject(ConnectionActorMsg(key, val)),
    cache: concurrent_dict.ConcurrentDict(key, val),
    cache_record_counter: concurrent_counter.ConcurrentCounter,
    cache_record_limit: Int,
  )
}

pub fn build(
  persist_path path: String,
  key_encoder key_encoder: fn(key) -> String,
  val_encoder val_encoder: fn(val) -> String,
  val_decoder val_decoder: fn(String) -> val,
  cache_record_limit cache_record_limit: Int,
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

  use conn_actor <- result.map(
    actor.new(ConnectionActorState(
      conn:,
      key_encoder:,
      val_encoder:,
      val_decoder:,
    ))
    |> actor.on_message(handle_persist_data)
    |> actor.start
    |> result.map(fn(actor) { actor.data })
    |> snag.map_error(string.inspect),
  )

  CacheLimitedPersistentConcurrentDict(
    conn_actor:,
    cache: concurrent_dict.new(),
    cache_record_counter: concurrent_counter.new(),
    cache_record_limit:,
  )
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

const select_query = "SELECT value FROM persist WHERE key = ?"

pub fn insert(pcd: CacheLimitedPersistentConcurrentDict(key, val), key, val) {
  // Do not cache the inserted data, we do not know of the user will get
  // it again any time soon. We only cache the data after it is read from
  // the disk so repeat
  process.call(pcd.conn_actor, 100_000, PersistData(key, val, _))
  |> snag.map_error(string.inspect)
}

pub fn get(pcd: CacheLimitedPersistentConcurrentDict(key, val), key) {
  use Nil <- result.try_recover(concurrent_dict.get(pcd.cache, key))

  // If the data is not in the cache, then try to get it from the disk
  use data <- result.try(
    process.call(pcd.conn_actor, 100_000, GetData(key, _))
    |> snag.map_error(string.inspect),
  )

  case
    concurrent_counter.value(pcd.cache_record_counter) >= pcd.cache_record_limit
  {
    // We could thoughtfully delete only some of the cache records here, but 
    // for now we can just delete all of them
    True -> {
      concurrent_dict.delete_all(pcd.cache)
      concurrent_counter.reset(pcd.cache_record_counter)
    }
    False -> Nil
  }

  // Once we got the data from the disk, add it to the cache for quicker access
  // in the future
  concurrent_dict.insert(pcd.cache, key, data)
  concurrent_counter.increment(pcd.cache_record_counter)

  Ok(data)
}

pub fn to_list(pcd: CacheLimitedPersistentConcurrentDict(key, val)) {
  concurrent_dict.to_list(pcd.cache)
}
