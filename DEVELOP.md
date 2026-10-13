# Developing easybus-sdk

## Toolchain

* Go **1.26+** (the module declares `go 1.26`). The sandbox image ships no Go
  toolchain; install one and put it on `PATH`, e.g.:
  ```sh
  curl -L -o /tmp/go.tgz https://go.dev/dl/go1.27.1.linux-amd64.tar.gz
  tar -C /usr/local -xzf /tmp/go.tgz
  export PATH=/usr/local/go/bin:$PATH
  ```
* Tests that exercise the NATS transport spawn a real `nats-server`. Install it
  (e.g. the release tarball into `/usr/local/bin`) or set
  `EASYBUS_NATS_SERVER_BIN`. Without it those tests `t.Skip`.
* `proxy.golang.org` is unreachable in-cluster. Modules are consumed from the
  private **artifact** registry — see
  [Packages via artifact](#packages-via-artifact-not-githubpublic-registries)
  below (or, as a stopgap for a non-cluster sandbox, a reachable mirror such as
  `GOPROXY=https://goproxy.cn,direct`).

## Packages via artifact (not GitHub/public registries)

All internal packages are published to and consumed from **artifact**
(`artifact.worker.svc.cluster.local`), not GitHub or public registries. This
repo is a **Go module producer** (`github.com/easydev-org/easybus-sdk`). The
authoritative guide is `easy-vcs/deploy:PUBLISHING.md`.

```sh
# Publish (producer): upload the module zip under its module path + tag.
curl -X PUT --data-binary @easybus-sdk.zip \
  "http://artifact.worker.svc.cluster.local/artifacts/go/upload?name=github.com/easydev-org/easybus-sdk&version=vX.Y.Z"

# Consume (downstream Go modules):
export GOPROXY=http://artifact.worker.svc.cluster.local/artifacts/go
export GOSUMDB=off
```

Reads are anonymous; writes need the artifact write token (see `PUBLISHING.md`).
Downstream repos that consume this module via a `github:` git dependency should
instead point at the cluster Forgejo or the artifact Go proxy:
`git+http://git.agent.svc.cluster.local/<org>/<repo>.git#<tag>`, or the
`GOPROXY` above. See `easy-vcs/deploy:PUBLISHING.md`.

## Build / test / lint

```sh
go build ./...
go vet ./...
gofmt -l .              # must be empty
go test ./...
```

The full suite starts one ephemeral broker per conformance subtest; it needs
the `nats-server` binary. `EASYBUS_NATS_URL` opts into a shared external broker
instead (used for live-cluster checks).

### Known pre-existing failure

`conformance/tool_object_offload` fails on pristine `main`
(`expected object offload, got {… Object:<nil> …}`). It is unrelated to
extension replica support — do not treat it as a regression, but also do not
"fix" it incidentally.

## Conventions

* **Zero/optional fields** — pointers for optional wire fields; `omitempty`
  tags. Generated types live in `types.gen.go` and are regenerated from the TS
  zod schemas via `generate.sh` — never hand-edit them.
* **Tenant namespacing** — every data-plane subject is `easybus.<tenant>.<…>`;
  discovery is the global exception (`easybus.discover`, `protocol.GlobalTenant`).
* **KV keys** — build them with the `protocol.*Key` helpers; escape untrusted
  segments with `protocol.EscapeKVSegment` (a `.` in a key splits it).
* **Queue groups** — every extension data-plane subscription uses
  `SubscribeOpts{Queue: extId}` so N replicas load-balance. Keep new
  subscriptions consistent with this.
* **Comments** explain *why*, not *what*; follow the existing dense-comment
  style in `extension/extension.go` and `agent/agent.go`.

## Multi-replica support (extension liveness)

Extensions may run N replicas under one `extId`. Two invariants make that safe,
and both are load-bearing:

1. **Discovery is queue-grouped.** `serveDiscovery` subscribes `easybus.discover`
   with `Queue: e.cfg.ID`. N replicas => exactly one reply per broadcast.
   (Covered by `TestDiscoveryQueueGroupSingleReply`.)
2. **Presence is per replica.** Each replica heartbeats the KV key
   `<extId>.<replicaId>`; `Close()` deletes only its own key. The agent keeps
   one manifest per `extId` and treats the extension as offline only when the
   last replica key is gone. (Covered by `TestPresencePerReplicaAndLiveness`
   and `conformance/presence_replica_keys`.)

### `replicaId`

Resolved by `resolveReplicaID` (see `extension/extension.go`), priority:
`Config.ReplicaID` → `EASYBUS_REPLICA_ID` → `POD_NAME` → random UUID. The result is
sanitized to `[A-Za-z0-9_-]` so it can never contain a `.`. Prefer `POD_NAME`
in Kubernetes (stable across restarts, no stale keys); do not inject
`EASYBUS_REPLICA_ID` in charts.

### Agent presence bookkeeping

`Agent` keeps `livePresenceKeys: map[extId]map[kvKey]bool` next to
`manifestCache: map[extId]manifest`. `applyPresenceEvent`/`trackPresenceKey`/
`forgetPresenceKey` maintain the invariant: **cache entry exists iff at least
one replica key is live**. `Discover()` returns one manifest per `extId`.
Legacy un-suffixed keys (`<extId>`) still parse via `extension.PresenceExtID`.

## Wire-format changes

The Go and TypeScript SDKs share the wire contract. A protocol-affecting change
must land in both, in lockstep, and be followed by a new `v2.x.y` tag (Go
semantic import versioning). Downstream repos consume the tag via the artifact
Go proxy (see [Packages via artifact](#packages-via-artifact-not-githubpublic-registries)),
not a public registry or github.com.
