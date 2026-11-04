# Event Queue: an in-memory message broker

A small publish-subscribe message broker written in Go, using only the standard library.

Messages are stored once in an append-only log. Each subscriber has its own read position (offset) into that log. This is the same model Kafka uses for topics and consumer offsets.

## Running it

Requires **Go 1.24+**.

```bash
go run ./cmd/demo          # run the example sequence from the brief
go test -race ./...        # run all tests with the race detector
```

Or with `make`: `make run`, `make test`, `make cover`.

### Demo output

The demo follows the seven steps in the brief. Subscriber 1 subscribes after five messages are published, and Subscriber 2 after all ten. Each one still prints all 10 messages in publication order, giving 20 lines:

```
[subscriber-1] offset=0 payload=message-1
...
[subscriber-1] offset=9 payload=message-10
[subscriber-2] offset=0 payload=message-1
...
[subscriber-2] offset=9 payload=message-10

Total message logs: 20

Checks:
  subscriber-1 subscribes again, then reads: 0 messages (re-subscribe is a no-op)
  subscriber-2 reads again: 0 messages (nothing consumed twice)
```

## Project layout

```
broker/
  broker.go        Broker: the message log, subscriber offsets, publish and read
  publisher.go     Publisher: writes messages to a broker
  subscriber.go    Subscriber: subscribes once, reads its own messages
  errors.go        Error values
  *_test.go        Unit and concurrency tests
cmd/demo/main.go   The example sequence from the brief
```

## Design

```
                 append-only log (stored once)
  Publisher ──▶  [0][1][2][3][4][5][6][7][8][9]
                                                 ▲
  offsets:   subscriber-1 ───────────────────────┘ 10  (read everything)
             subscriber-2 ─▶ 0                         (has not read yet)
```

- **`Broker[T]`** holds a slice of messages (`log`) and a map from subscriber ID to offset (`offsets`). A single `sync.Mutex` guards both.
- **Publish** appends to the log. A message's offset is its index in the log, so the offset also records publish order.
- **Read** returns `log[offset:]` (or up to `max` messages), then moves the subscriber's offset to the end of what it returned. Both happen under the same lock.
- **`Publisher`** and **`Subscriber`** are thin types over the broker. They give the API the brief asks for while keeping all state in one place.

## Design decisions and trade-offs

**One shared log plus offsets, not a queue per subscriber.** Copying each message into every subscriber's queue costs memory per subscriber and means a late subscriber can't see older messages. With one log, a message is stored once, and a new subscriber is just a new offset.

**The broker stores the offsets, keyed by subscriber ID.** The subscriber still has its own read position, but the broker holds it, as Kafka does with consumer offsets. Callers can't skip or rewind by mistake. It also means the broker knows how far every subscriber has read, which features like acknowledgements and log cleanup need.

**New subscribers start from the beginning of the log.** The brief expects Subscriber 1, which subscribes after five messages, to print all ten. So a subscription replays the full history (Kafka calls this `earliest`). Starting at the end of the log ("only new messages") would be an option to add.

**Pull-based reads.** Subscribers call `Read` when they're ready. That matches "read and print" in the brief, makes ordering easy to reason about, and gives natural back-pressure, because a slow subscriber just falls behind. `max` lets a caller read in bounded batches.

**One mutex, not a read-write lock.** `Read` changes the subscriber's offset, so every operation writes and a read-write lock would add nothing. The lock is only held for a slice copy or an append, so contention is low. If it ever mattered, one option is to keep each subscriber's offset under its own lock and guard the log with a read-write lock.

**Reads return a copy.** `Read` copies messages out of the log, so a caller changing the returned slice can't change what other subscribers see. Payloads that are pointers or maps are still shared. The broker doesn't deep-copy them.

**Subscriber IDs must be unique per broker.** Two subscribers with one ID would share an offset and steal each other's messages, so `Subscriber.Subscribe` returns `ErrSubscriberIDTaken`. A subscriber attaches to one broker; subscribing it to a second returns `ErrAlreadySubscribed`.

**Errors, not panics.** Reading before subscribing returns `ErrNotSubscribed`. All errors are exported values, so callers can check them with `errors.Is`.

## Testing

`go test -race ./...` runs:

- **Unit tests** for each requirement: ordering, at-most-once delivery, no-op re-subscribe (offset kept), independent offsets, late subscribers, read limits, defensive copies, struct payloads, and error cases.
- **The example sequence** from the brief, asserting both subscribers get all 10 messages and the total is 20.
- **Concurrency tests** under the race detector: 8 publishers and 4 subscribers running at once (every subscriber sees every offset exactly once, in order), batches staying contiguous, and 100 goroutines subscribing one ID with exactly one succeeding.

