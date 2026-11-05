# Latency: what I measured, what I refused to claim

Extension 4 asks for experiments on getting faster connections. Below are the numbers I
actually collected, and the one number I am not going to invent.

## What the measurements say

From the machine that built this (Seattle, US; 3 ms TCP round trip to Kalshi), 20
sequential requests per endpoint, TLS 1.3:

```
endpoint                      dns   tcp   tls   first-byte   total   (p50, ms)
api.elections.kalshi.com       2     4    26        33         33
external-api.demo.kalshi.co    1    11    33        45         45
clob.polymarket.com            2     4    27       185        185
```

Three findings, in order of usefulness.

**1. Connection setup is not the bottleneck I expected, once you are close.** TLS is 26 ms
of a 33 ms request, and 22 ms of that is the two round trips the handshake costs at 3 ms
RTT plus the exchange's own compute. There is no clever tuning hiding in there. The only
lever that scales is round-trip distance, and that is a hosting decision, not a code
decision.

**2. Spamming connections makes each one slower, not faster.** This is the part of
Extension 4 I could test honestly. Forty simultaneous TLS handshakes to the same host:

```
n=40 parallel   tls p50 114 ms  p90 161 ms     total p50 131 ms  p90 176 ms
n=20 serial     tls p50  26 ms  p90  28 ms     total p50  33 ms  p90  36 ms
```

Every connection succeeded (40/40 HTTP 200, no rate-limit response), but per-connection
handshake cost rose 4.4x. That is consistent with the server or the path serialising
handshake work. The practical conclusion is the opposite of "spam connections until you
find a fast one": a *warm* standby opened before you need it is worth more than fifty cold
ones opened at the same moment, which is exactly why this collector keeps a standby
subscribed rather than dialling on failure.

**3. Polymarket is a different shape of problem.** Its TLS setup is as fast as Kalshi's,
but first-byte is 185 ms against 33 ms. Connection tuning would buy at most 5 percent
there; the venue's own processing dominates. Any latency work on Polymarket has to start
with what the endpoint does, not with the socket.

## What I deliberately did not claim

**Publish-to-apply latency, measured end to end.** I could produce a number, and it would
be a measurement of geography, not of this code. The sandbox is in Seattle; Irfan runs the
collector from Kuwait, roughly 200 ms of round trip from Kalshi's US endpoints. A p99 that
is 90 percent distance would tell a reviewer nothing about the sort of service they are
asking for, and dressing it up as a result would be the dishonest part of this challenge.

What the collector does instead is make the measurement possible downstream. Every record
carries two clocks:

* `exchange_timestamp`, from the exchange's own `sending_ts_ms` (and the payload timestamp
  where the venue provides one), and
* `backtest_timestamp`, the local instant the message was parsed.

Their difference, per record, is the observable-latency distribution for whatever machine
ends up running this. A backtester can compute p50/p99 from the file without trusting any
number I asserted from a datacentre the production collector will never use.

## If I were optimising for real, in order

1. **Co-locate.** Rent in the same metro as the venue's endpoint. At 3 ms RTT the whole
   problem shrinks to what the exchange controls; at 200 ms nothing else matters.
2. **Keep the standby warm**, which this collector already does. The measured 4.4x
   handshake penalty under concurrency says a cold dial at failover time is the expensive
   path.
3. **Pin the source port and reuse the resolved address.** `getaddrinfo` was 1 to 2 ms
   here, so the gain is small locally, but it removes a DNS dependency from the failure
   path, which matters more than the milliseconds.
4. **Timestamp on arrival, before parsing.** The parser runs after `Utc::now()` is taken in
   this code, so decode time is not charged to the exchange. If it were, a burst would
   inflate measured latency with our own CPU.
5. **Do not bother with TLS tuning** at this scale. TLS 1.3 with session resumption is
   already what `rustls`/`tungstenite` negotiate, and the handshake is a fixed cost you pay
   once per connection, not per message. I could not demonstrate session-ticket reuse from
   my tooling, so I am not claiming a win there either.

## Reproducing

```
# percentiles per endpoint (curl, 20 samples)
for i in $(seq 1 20); do curl -sS -o /dev/null \
  -w "%{time_namelookup} %{time_connect} %{time_appconnect} %{time_starttransfer} %{time_total}\n" \
  https://api.elections.kalshi.com/trade-api/v2/exchange/status; done

# the concurrency experiment: 40 at once, then compare with the same loop serially
for i in $(seq 1 40); do curl -sS -o /dev/null -w "%{http_code} %{time_appconnect} %{time_total}\n" \
  https://api.elections.kalshi.com/trade-api/v2/exchange/status & done; wait
```

Run it from wherever the collector will actually live, and the first-byte column becomes
meaningful in a way my numbers are not.
