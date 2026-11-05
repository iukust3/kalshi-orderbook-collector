# Kalshi orderbook collector, submission by Irfan Ullah

Start with [NOTES.md](NOTES.md): design, the three invariants, every decision worth
defending, the four extensions and the known limits. [LATENCY.md](LATENCY.md) has the
measured connection-setup numbers and the method.

```
cargo test --release                        # 26 tests, no network needed
MOCK=1 OUTPUT_PATH=stream.sbe cargo run --release   # offline, synthetic traffic
KALSHI_API_KEY_ID=... KALSHI_PRIVATE_KEY_PATH=key.pem OUTPUT_PATH=stream.sbe \
  cargo run --release                       # live Kalshi
```

Layout: `feed.rs` and `kalshi/` talk to the venue, `supervisor.rs` owns shards, standby
sockets and promotion, `book.rs` holds the order book maths, `pipeline.rs` is the single
writer, `sink.rs` writes the SBE chunks, `polymarket/` is the second-venue adapter.

Everything below is the starter repository's own README.

---

# Read before getting started!!

First, read [src/main.rs](https://github.com/voluntas-io/engineering-challenge/blob/main/src/main.rs). Then, some background on this repo:

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away for you, and you can simply use the helpers, as demonstrated in `main.rs`.

### More background, in case you are curious!

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](https://github.com/voluntas-io/engineering-challenge/blob/main/sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.

