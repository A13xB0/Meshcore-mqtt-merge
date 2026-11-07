# meshcore-mqtt-merge

Subscribes to several MeshCore MQTT brokers and republishes everything to one output broker, so tools like [CoreScope](https://github.com/Kpa-clawbot/CoreScope) only need a single feed.

Observers often upload to more than one broker. Without care, each of their packets would arrive once per broker. The merger handles that:

| Situation | What happens |
| --- | --- |
| Same observer, same packet, several brokers | Forwarded once. Matched on observer key and the `raw` packet bytes, so different uploaders or timestamps still match. |
| Different observers hear the same packet | All kept. Each one is a separate reception with its own path and SNR. |
| Observer uses a different IATA code on each broker | Pinned to the code from the earliest input in the config that carries it, so it doesn't jump between regions. |
| Observer drops off one broker (LWT `offline`) but is still online on another | The `offline` is held back. It goes out once no input still sees the observer online. |
| An input broker connection drops | That broker's "online" claims are forgotten, so they can't hold back a real `offline`. |
| `status` messages | Republished retained, like the observers publish them. |
| Anything else (`meshcore/client/...`, `meshcore/status`, ...) | Passed through, with identical topic + payload deduplicated. |

Dedup applies within `dedupSeconds` (default 60).

## Run

```sh
cp config.example.json config.json   # edit
go run . -config config.json
```

or with Docker:

```sh
docker compose up -d --build
```

`${VAR}` in the config is filled from the environment, so passwords can stay in `.env`.

## Config

| Key | |
| --- | --- |
| `inputs[]` | `name`, `url` (`mqtt://`, `mqtts://`, `ws://`, `wss://`), `username`, `password`, `clientId`, `insecure` (skip TLS verify), `topics` (default `["meshcore/#"]`). Earlier inputs win when pinning an observer's region. |
| `output` | Same connection fields, plus `qos` and `topicRoot`. |
| `output.topicRoot` | Replaces the first topic segment, e.g. `merged` → `merged/EDI/<key>/packets`. Required if the output broker is also an input, otherwise the merger would read its own output. |
| `dedupSeconds` | Duplicate window. |
| `statsSeconds` | How often per-input counts (`in`, `out`, `dup`, `suppressed`) are logged. |

Messages that arrive while the output is disconnected are dropped and counted in the logs.

## Test

```sh
go test -race ./...
```

The end-to-end test starts three in-process brokers, publishes the same packet from one observer to two of them, and checks it comes out once.
