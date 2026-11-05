# Orderbook collector: design notes

Irfan Ullah. Kalshi orderbook collector in Rust, built on the provided SBE schema and
`AlignedSbeWriter`.

Run it:

```
# live
KALSHI_API_KEY_ID=... KALSHI_PRIVATE_KEY_PATH=key.pem OUTPUT_PATH=stream.sbe \
  cargo run --release

# offline, no credentials, deterministic synthetic traffic
MOCK=1 OUTPUT_PATH=stream.sbe cargo run --release

cargo test --release        # 26 tests, no network
```

Every knob is an environment variable (`src/config.rs`): `SOCKETS`, `SUBSCRIBE_BATCH`,
`STALE_SECS`, `RECYCLE_SECS`, `SNAPSHOT_INTERVAL_SECS`, `OUTPUT_CHUNK_BYTES`,
`OUTPUT_FLUSH_MS`, `OUTPUT_FLUSH_MIN_BYTES`, `CATALOG_REFRESH_SECS`, `NO_REDUNDANCY`,
`MOCK`, `KALSHI_ENV=demo|prod`.

## Credentials

Kalshi API keys are free and created in the account dashboard: Profile settings ->
API Keys -> Create New API Key (`https://kalshi.com/account/profile`). Kalshi shows the
Key ID and the private key PEM once. Either key type works here: Ed25519 (PKCS#8) or
RSA-2048 (PKCS#1 or PKCS#8). The signer parses the key and picks the matching algorithm
instead of trusting the PEM header, because Kalshi's RSA download is PKCS#1 while
`openssl genpkey` produces PKCS#8 with the same header as an Ed25519 key. Three tests cover
that: RSA PKCS#8, RSA PKCS#1, and Ed25519 detected from the same `BEGIN PRIVATE KEY` header.

The demo environment needs no funding: an account at `https://demo.kalshi.co` with mock
money is enough, and the collector already points at the recommended demo roots
(`external-api.demo.kalshi.co`, `wss://external-api-ws.demo.kalshi.co`) under
`KALSHI_ENV=demo`. Credentials are not shared between demo and production.

## Shape

```
REST /markets ──> catalog ─┐
                           ├─> supervisor ─> feed worker x N shards x (primary + standby)
ws market_lifecycle_v2 ────┘        │                     │
                                    │                     v
                                    └──────> pipeline (single writer) ──> SBE chunk writer
```

## The three invariants

1. **One writer.** Every book lives in one task (`pipeline.rs`), and only that task touches
   the sink. Workers send `BookEvent`s over a channel. This is what makes the output stream
   totally ordered: no two sockets can interleave, and a replay never sees a delta before
   the snapshot it depends on.
2. **Absolute quantities.** `PriceLevelUpdate` carries the quantity *at* a price after the
   change, not the source delta. `book.apply` converts. A consumer that reads the file can
   rebuild the book without knowing anything about Kalshi.
3. **Backpressure, never drop.** The event channel is large and workers `.await` on it. If
   the writer falls behind, the sockets slow down. Nothing is discarded to keep up, because
   a gap in an order book is corruption, not latency.

## Decisions worth defending

**Chunk size is 64 MiB, not the 256 KiB I started with.** `AlignedSbeReader` walks the file
in fixed 64 MiB strides and treats zeros as end-of-chunk, so any other chunk size produces a
file the provided reader cannot decode. I only found this by round-tripping the format in
`tests/stream_contract.rs`, which is why that test exists.

**Flushing is explicit about its cost.** A flush pads the current chunk to full size, so
flushing a 20 KB buffer writes 64 MiB. `Sink::flush(FlushMode)` takes `IfWorthIt` (scheduled)
or `Force` (shutdown), and `OUTPUT_FLUSH_MIN_BYTES` sets the threshold. Default is 8 MiB, so
the worst padding waste per flush is 56 MiB and the file never grows by more than one chunk
between real data.

**Shards are round-robin, not contiguous.** Markets listed in the same event are correlated
and get busy together. Round-robin spreads a hot event across sockets instead of loading one.

**The standby receives but never writes.** It holds the same subscription and validates its
own `seq`, so failover does not need a resubscribe round trip. It is not a *source*, so its
frames never reach the sink and liquidity cannot be double-counted.

**Dedupe is keyed on `(clock, worker)`.** Kalshi's `clock` is monotonic per market and shared
across connections, but a single exchange action can emit several deltas with the *same*
clock. So equal clock from the same socket is two real changes, and equal clock from a
different socket is the redundant copy. A snapshot sets a floor that summarises everything at
or below its clock. The failing test in `pipeline.rs` is exactly this bug, caught before it
reached a live run.

**Promotion asks for snapshots.** A promoted standby's next delta would land on a book it
never contributed to, so `switch_source` sends `update_subscription(get_snapshot)` for the
shard. The book resets, the clock floor re-establishes, and the handover is lossless.

**Sequence gaps are repaired, not logged.** `feed` reports a `seq` jump; the supervisor
answers with a snapshot request for that shard. A log line does not fix a book.

**The universe has two sources on purpose.** `market_lifecycle_v2` is instant but loses
events across a reconnect; the REST catalog is complete but up to `CATALOG_REFRESH_SECS`
stale. Both feed the same idempotent add/remove path, so a missed event delays a market by at
most one refresh interval.

**Prices are parsed without floating point.** Kalshi sends `"0.0800"` and `"-54.00"`;
`parse_fixed_6` parses to 1e-6 fixed point by string surgery, with tests. Money and floats do
not mix.

## Extensions

**1. Hot-standby socket: done.** `supervisor.rs`, one standby per shard, source flag,
promotion with snapshot resync, verified by `NO_REDUNDANCY` toggling and the dedupe tests.

**2. Monitoring subscription rate limits: done, partially adaptive.** What exists: paced
subscribe bursts (`SUBSCRIBE_BATCH`, 20 ms between messages), per-shard staleness detection
that treats "connected but silent" as dead, exchange error surfacing, and periodic recycling
of the non-serving socket so cumulative subscription bugs cannot outlive an hour of uptime.
What I deliberately did not add: automatic batch shrink on a rate-limit error. Kalshi's error
codes are not documented well enough for me to safely distinguish "too many subscriptions"
from "bad ticker", and guessing would silently shrink coverage. The log line names the code
and the shard, so the next iteration can key on the real one.

**3. Polymarket adapter: shipped at the pipeline boundary.** `src/polymarket/mod.rs` decodes
both envelope shapes Polymarket serves (the flat `{event_type, asset_id, ...}` stream and the
newer `{topic, type, payload}` SDK shape), normalises them, and emits the same `BookEvent`s
the Kalshi feed emits, so the pipeline, redundancy, snapshot cadence and SBE writer are
shared unchanged. `tests/polymarket_stream.rs` proves it end to end: real Polymarket frames
go through the shared pipeline into the SBE file and come back out of the challenge's own
reader. Two venue differences are handled in the adapter rather than leaking upstream:
Polymarket publishes *resting size* rather than a signed change, which maps to the new
`BookEvent::SetLevel` (a delta interpretation would turn 300-then-200 into 500); and its
messages identify a market by opaque 77-digit asset id, so the shard key is that id.
Subscription uses their spelling `assets_ids`, and a literal `PING` keeps the socket alive.
Honest scope note: the socket loop, catalog and supervisor remain Kalshi-specific, so this
is the decode/encode boundary proven, not a second live venue. Wiring it is a socket loop
plus an asset list source, roughly 80 lines, and `feed`'s subscription bookkeeping is the
only part that assumes Kalshi sids.

**4. Latency: measured what is measurable, refused the rest.** Full numbers and method are
in `LATENCY.md`. The two that matter: TLS setup is 26 ms p50 against Kalshi from a 3 ms-RTT
host, and 40 simultaneous handshakes raise that to 114 ms p50, a 4.4x penalty, which is the
measured argument against "spam connections until you find a fast one" and the argument for
the warm standby this collector already keeps. I did not publish a publish-to-apply p99,
because the sandbox sits in Seattle and Irfan runs the collector from Kuwait: that number
would measure geography. Instead every record carries both clocks, exchange and local, so
the latency distribution can be computed from the output on the machine that matters.

## Known limits

* One process, one file. No crash recovery beyond "restart and resubscribe", which is
  correct because snapshots are authoritative.
* No credential refresh: the RSA signature is per connection, which is enough for a
  long-lived collector.
* `market_closed` does not remove a market (it still has liquidity until settlement);
  `market_deactivated`, `market_settled` and `market_determined` do.
* The Polymarket adapter is proven at the pipeline boundary, not against a live socket
  (see Extension 3). Its market stream also needs a `PING` every 10 s to stay open.
* The mock's rate is fixed at 20 events/s, so it proves the writer and the invariants, not
  throughput. Throughput on real traffic is bounded by the single writer, which is the
  deliberate trade for ordering.
