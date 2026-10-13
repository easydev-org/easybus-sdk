# easybus-sdk

The Go SDK for **easybus**, the multi-tenant, bus-native protocol that
connects an *agent* (the orchestrator) to *extensions* (tools, variables,
hooks). A shared NATS/JetStream transport carries a versioned envelope,
tenant-namespaced subjects, and a KV/object-store side channel.

Module: `github.com/easydev-org/easybus-sdk`

## Packages

| Package | Role |
| --- | --- |
| `protocol` | Subject builders, tenant namespacing, KV-key escaping, small arg helpers |
| `bus` | Transport-agnostic `Bus` interface (request/requestMany/publish/subscribe, KV, object store, inbox) |
| `transport/nats` | The NATS/JetStream implementation of `Bus` |
| `extension` | Extension-side role: `Extension`, `Config`, manifest, presence, config store |
| `agent` | Agent-side role: `Agent` (discover, call tools, hooks, config authority, mailbox, leases) |
| `manifest` | YAML manifest parsing + `BuildConfig` bindings |
| `i18n` | Locale resolution / interpolation |
| `identity` | Opt-in HMAC message authentication |
| `natsrun` | Launch a local `nats-server` for embedded/tests |
| `conformance` | Transport-agnostic protocol test suite |
| `gen/easybus/v1` | Generated wire types (from `proto/easybus/v1`) |

## The wire contract is protobuf

`proto/easybus/v1/protocol.proto` is the **single source of truth** for every
message that rides the bus. It is language-neutral: `buf generate` emits Go
today (`gen/easybus/v1`), and TypeScript / Python / Rust bindings later, from
the same file — no per-language schema to keep in sync. (The old
abc-protocol defined the contract in TypeScript zod and code-generated Go via
OpenAPI; this is a semantic rewrite, and wire compatibility with abc-protocol
is not required.)

Subject conventions (`easybus.<tenant>.<...>`) and tenant rules are runtime
logic, not schema, and stay in `protocol/protocol.go`.

```sh
./generate.sh    # buf lint + buf generate + gofmt
```

## Install

```sh
go get github.com/easydev-org/easybus-sdk@latest
```

## Quick start

Extension side (see `examples/serve-demo`):

```go
nbus, _ := nats.Connect(natsAddr)
ext := extension.New(nbus, cfg)         // or manifest.BuildConfig(...)
err := extension.Serve(ext, extension.ServeOptions{ /* Handler, Run */ })
```

Agent side:

```go
a := agent.New(nbus)
manifests, _ := a.Discover(ctx, 500)                 // presence-first, broadcast fallback
res, _ := a.CallTool(ctx, tenant, session, "ops", "echo", callID, args)
```

## Running extensions with multiple replicas

Extensions may run **N replicas behind the same extension id** (`workspace`,
`playwright`, …). The SDK supports this natively:

* **Data plane** — tool/variable/hook/lifecycle/interrupt subscriptions all
  subscribe under the queue group `<extId>`, so NATS load-balances each request
  to exactly one replica.
* **Discovery** — the `easybus.discover` broadcast is also subscribed under the
  queue group `<extId>`, so a discovery request is answered **once** per
  extension, not once per replica.
* **Liveness (presence)** — each replica heartbeats its **own** key
  `<extId>.<replicaId>` into the `easybus-presence` KV bucket (TTL 15s, refreshed
  every 5s). An extension is *alive* while any `<extId>.` key is unexpired, and
  *offline* only once **all** of them are gone. A replica's `Close()` deletes
  only its own key, so killing one replica never marks the extension offline.

### `replicaId` resolution

The replica id is resolved once per process, highest priority first:

1. **`EASYBUS_REPLICA_ID`** — explicit override (debugging, pinning an identity).
   Also settable programmatically via `extension.Config.ReplicaID`.
2. **`POD_NAME`** — the k8s downward-API pod name. This is the default in a
   cluster: it is unique, **stable** across a pod's restarts (so no stale key is
   left behind), and DNS-safe. **Charts need no change** — do not inject
   `EASYBUS_REPLICA_ID`.
3. **A random id** — non-k8s / local runs; the 15s TTL reclaims old keys.

The id is sanitized to the KV-key alphabet (`[A-Za-z0-9_-]`); any `.` becomes
`-`, so a replica id can never split the `<extId>.<replicaId>` key.

## Deploying

Extensions connect to NATS as clients; the agent may embed the broker via
`natsrun`. Set `NATS_URL` (see `examples/serve-demo`) and, in Kubernetes, ensure
`POD_NAME` is exposed (the downward API does this by default).

## Versioning

Tags are semantic import versioning. The wire contract is defined once by
`proto/easybus/v1/protocol.proto` (`gen/easybus/v1/*.pb.go` is generated from
it, in every language); a protocol-affecting change updates the `.proto` and
regenerates. Internal packages are published to / consumed from the platform's
private **artifact** registry; see `DEVELOP.md`.
