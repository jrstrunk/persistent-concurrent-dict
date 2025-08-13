import gleam/erlang/process
import gleam/int
import gleam/otp/actor
import gleam/result
import gleeunit
import gleeunit/should
import persistent_concurrent_dict as pcd

pub fn main() {
  gleeunit.main()
}

pub fn hello_world_test() {
  let assert Ok(pcd_test) =
    pcd.build(":memory:", fn(x) { x }, fn(x) { x }, fn(x) { x }, fn(x) { x })

  pcd.get(pcd_test, "hello")
  |> should.equal(Error(Nil))

  let assert Ok(Nil) = pcd.insert(pcd_test, "hello", "world")

  pcd.get(pcd_test, "hello")
  |> should.equal(Ok("world"))
}

pub fn number_test() {
  let assert Ok(pcd_test) =
    pcd.build(":memory:", fn(x) { x }, fn(x) { x }, int.to_string, fn(x) {
      let assert Ok(i) = int.parse(x)
      i
    })

  pcd.get(pcd_test, "hello")
  |> should.equal(Error(Nil))

  let assert Ok(Nil) = pcd.insert(pcd_test, "hello", 1)

  pcd.get(pcd_test, "hello")
  |> should.equal(Ok(1))
}

type Msg {
  Set(String)
  Get(reply: process.Subject(String))
}

pub fn subscribe_test() {
  let assert Ok(pcd_test) =
    pcd.build(":memory:", fn(x) { x }, fn(x) { x }, fn(x) { x }, fn(x) { x })

  let assert Ok(dep) =
    actor.new("")
    |> actor.on_message(fn(state, msg) {
      case msg {
        Set(str) -> actor.continue(str)
        Get(reply) -> {
          process.send(reply, state)
          actor.continue(state)
        }
      }
    })
    |> actor.start
    |> result.map(fn(actor) { actor.data })

  pcd.subscribe(pcd_test, fn() { process.send(dep, Set("update")) })

  pcd.subscribe_to_key(pcd_test, "hi", fn() {
    process.send(dep, Set("update_key"))
  })

  let assert Ok(Nil) = pcd.insert(pcd_test, "hello", "world")

  process.sleep(10)

  let str = process.call(dep, 100_000, Get)

  should.equal(str, "update")

  let assert Ok(Nil) = pcd.insert(pcd_test, "hi", "world")

  process.sleep(10)

  let str = process.call(dep, 100_000, Get)

  should.equal(str, "update_key")
}
