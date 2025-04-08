import gleam/int
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
