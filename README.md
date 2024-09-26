# KV Store Using Rust

This is a single node simple key value store that uses `sqlite` for durability. The following performance techniques have been added so far:
[x] Multi-threaded
[x] Connection pooling
[x] Thread safe LRU cache (using moka)
[x] Write ahead logging

To run this project, you can run the following commands:

```cargo build```
followed by:
```cargo run```