# easybus-sdk

The Go SDK for **easybus**, the multi-tenant, bus-native protocol that
connects an *agent* (the orchestrator) to *extensions* (tools, variables,
hooks). A shared NATS/JetStream transport carries a versioned envelope,
tenant-namespaced subjects, and a KV/object-store side channel.

Module: `github.com/easydev-org/easybus-sdk`

## Why this is `easybus` and not `easyagent`

**`easybus` IS THE PROTOCOL'S NAME, NOT THE PRODUCT'S.** Everything that depends on this module is an
EasyAgent component — `easyagent`, and the gitdev, ops, playwright and sandbox extensions — so
"easyagent-sdk" would describe the consumers accurately. It would also be wrong about the thing being
named.

The name is not only a repository title. It is the same word in three places a rename cannot reach
cheaply:

| where | value |
|---|---|
| module path | `github.com/easydev-org/easybus-sdk` |
| protobuf package | `easybus.v1` |
| **every NATS subject** | `easybus.<tenant>.<…>` |

Renaming the module would mean editing the import path in five repositories and re-resolving every
published tag. Renaming the subject prefix is worse: it is a **breaking wire change**, and an agent and
an extension on different sides of it cannot see each other at all.

So the split stays: **EasyAgent is the product, easybus is how its parts talk.** A transport layer with
its own name is the ordinary arrangement — the alternative is a name that has to be explained every
time the protocol and the product change at different speeds.

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

## There is no `job` package, and that is deliberate

THIS REPOSITORY USED TO CARRY ONE: a `Registry` over a KV bucket with a heartbeat, a sweeper that
marked stalled entries lost, and a four-state vocabulary (`running`, `done`, `cancelled`, `lost`)
meant to describe any long-running work. The console listed a conversation's jobs from it, and every
extension registered into it.

IT WAS REMOVED BECAUSE THE DOMAINS DO NOT SHARE A SHAPE. A sandbox command is a process this extension
owns, so cancelling it kills something and losing contact means it is gone. A GitHub workflow run is a
process GitHub owns: cancelling it is a *request* that may be declined, and the extension restarting
does not stop the run at all. Pressing both into one four-state vocabulary cost the distinctions that
mattered — GitHub's `action_required` (waiting for a human) had no state to land in, and `lost` was a
lie about a run that was still going.

WHAT REPLACED IT: each extension owns its own job tools and its own state words, so
`sandbox-job-output` reads the worker's buffer and `gh-run-log` reads GitHub — each answering from the
place that actually knows. `sessionstore` still provides the per-session list shape for anyone who
wants one; it just no longer pretends two different kinds of work are the same list.

WHAT DID NOT GO WITH IT: the completion wake (a finished job publishing a mailbox trigger so the
conversation is not left polling) lives in `extension.PublishMailboxEvent` and never depended on this
package.

## The wire contract is protobuf

`proto/easybus/v1/protocol.proto` is the **single source of truth** for every
message that rides the bus. `buf generate` emits Go into `gen/easybus/v1`, and
Go is the ONLY implementation.

**THE MULTI-LANGUAGE PLAN WAS ABANDONED, and the reasons are worth keeping.**
TypeScript, Python and Rust bindings were built and are gone: the TypeScript
SDK had re-implemented subjects, KV keys, envelope rules, discovery and
permissions that this repository already had, and the two drifted — the one
extension still using it sat six versions behind on the wire while its own
tools kept working, which is a gap nothing fails on. It is now a Go shell with
a browser sidecar, so a protocol change is a change to this repository only.

The proto stays language-neutral on purpose: if a second language is ever
genuinely needed, the contract is ready and `proto/buf.gen.yaml` gains a
plugin. What is gone is the assumption that every language must exist.
(The old abc-protocol defined the contract in TypeScript zod and
code-generated Go via OpenAPI; this is a semantic rewrite, and wire
compatibility with abc-protocol is not required.)

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
