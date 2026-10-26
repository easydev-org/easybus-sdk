# End-to-end tests for easyagent extensions

How to test an extension against **real** services, with **no agent**, in CI.

Every extension repo here follows this shape. Read it before adding an e2e suite, and before
"fixing" one that seems flaky.

---

## The one idea

> A tool call is an ordinary NATS request on
> `easybus.<tenant>.tool.call.<extId>.<tool>`. The agent is only ONE client of that subject.

So a test can be the agent. There is no agent process, no database, no session — just the extension,
a real broker, and a client that speaks the same request.

**Why this is not a shortcut:** `callTool` uses `Request`, which sets `ReplyTo`. The extension
**silently ignores** a call with no `ReplyTo`:

```go
if env.ReplyTo == nil {
    continue        // no log, no error, no reply
}
```

A hand-rolled `Publish` would therefore look like a timeout that names nothing. Use the SDK client.

---

## What each extension's suite looks like

| repo | services in compose | what the suite proves |
|---|---|---|
| `easyagent-playwright-ext` | nats, **selenium/standalone-chrome** | a real browser navigates and a screenshot's bytes reach the object store |
| `easyagent-github-ext` | nats | a **real clone** of `easydev-org/test`, and an impossible patch is refused |
| `easyagent-sandbox-ext` | nats, **easyworker (labwc)** | a file written through the extension is read back **from the sandbox** |
| `easyagent-gitea-ext` | **(a)** nats+forgejo, **(b)** nats+easyworker | (a) the MR review workflow; (b) git runs **inside** the sandbox |

---

## Rules that were learned the hard way

### 1. The driver must be able to FAIL

`interop/live` printed tool errors and carried on, so every broken run exited 0 — **a green CI badge
over zero working tool calls**. A test that reports success while everything fails is worse than no
test: a reviewer cannot tell the two apart.

```go
if tr.Error != nil {
    fmt.Fprintf(os.Stderr, "%s: toolerr %s\n", tool, tr.Error.Message)
    os.Exit(1)              // NOT: print and continue
}
```

### 2. "No error" is not "it worked"

A tool answering with an empty string passes an `err == nil` check. Assert on the **content**:

```go
if strings.TrimSpace(cloneOut) == "" {
    os.Exit(1)   // the sandbox never checked anything out
}
```

### 3. Use a per-run nonce, or a cache can pass for you

Write then read, with content that differs every run. A constant passes even when the read is
served from memory or hard-coded.

### 4. Wait for REGISTRATION; do not sleep

`Serve` installs subscriptions **asynchronously**. A call issued right after it returns gets
`no responders` — indistinguishable from "this extension has no such tool".

```go
waitRegistered(t, natsURL)   // poll Discover() until the manifest appears
```

The same applies to config: a KV write is asynchronous, so the first call after it can race the
watch. Poll a tool that depends on the config, not `time.Sleep`.

### 5. Permission-gated tools need grants

Every tool with `requires:` in the manifest is refused with
`permission denied: <key> (no grant held)` **before** any work happens. Pass what an operator would
issue:

```go
agent.CallToolWithGrants(ctx, tenant, session, extID, tool, callID, args,
    []*easybus.Grant{{Key: "file.write", Params: easybus.Struct(map[string]any{"sandbox": "pod-a"})}})
```

Only for tools that declare it. A spare grant hides a manifest that stopped gating.

### 6. Healthchecks fail misleadingly when their command is missing

`selenium/standalone-chrome` and the easyworker images have **neither `curl` nor `wget`**. A
healthcheck whose command does not exist reports **unhealthy**, while the container log plainly says
`listening on 0.0.0.0:48080`. Prefer `python3` (present in both) or bash's `/dev/tcp` (via
`bash -c`, because Debian's `/bin/sh` is dash).

### 7. Start detached and probe; never `up --wait`

With `--wait`, a failing healthcheck reports only *"container is unhealthy"* and the reason is
buried in docker's pull output. Starting detached lets the next step print the health state **and
the container log** for whichever service failed.

### 8. Docker containers need claiming

An easyworker started without `WORKER_TOKEN` boots **UNCLAIMED** and prints an enrollment code
instead of accepting calls. Set the token at boot, and pass the same value in the extension's
`sandboxes` config.

### 9. Assert the contract, not your assumption

`list-sandboxes` with no config returns *"none configured"* — it does **not** refuse. An early
version of that test asserted a refusal and failed against correct behaviour. Read the handler
before writing the assertion.

### 10. Only real services, and only the ones that cannot be faked

Put in compose **only what the extension genuinely cannot fake**:

* **playwright** needs a browser — Selenium's image is the real thing.
* **github-git** needs git and credentials — clone `easydev-org/test` for real.
* **sandbox** needs an easyworker — use the published `sandbox-wayland` image.
* **Nothing** needs S3: screenshots go through the **transient** object bucket, which is NATS by
  design (in-flight hand-off, 24h TTL). Persistent bytes require S3, but no extension e2e writes one.

If a request can only be answered by the agent (e.g. `file.ingest`), answer it in the test — but
**read what the extension actually wrote** rather than returning a canned value:

```ts
const data = await bus.objectGet(signal, tenantObjectName(tenant, req.object))
if (!data?.length) return { ok: false, error: { code: 'not_found', message: '…' } }
res = { ok: true, code: req.code ?? `file-${req.object}`, mime: sniffMime(data) }
```

---

## The wiring, in four pieces

```
.github/ci/compose.yaml      only the services that cannot be faked
interop/e2e/ (or tests/)     the suite: SDK client + assertions
ci.yml -> a `live` job       start detached, probe, run, dump logs on failure
skip when the env is absent  so the unit job needs no services
```

The gate is deliberate:

```go
if os.Getenv("E2E_NATS_URL") == "" {
    t.Skip("E2E_NATS_URL not set")
}
```

A test that fails for want of a broker teaches people to ignore red. A test that *skips* quietly is
also a problem — check the log says which cases skipped and why.

---

## Writing one: the checklist

1. **Read the manifest.** Tool names, `input_schema` properties, `requires:`, `required_config:`.
   Guessing a tool name gives `no responders`, which reads like a broken extension.
2. **Read the handler** for anything you intend to assert, especially refusals.
3. Add the service to `compose.yaml` — nothing more than needed.
4. Write the suite with the SDK client; one test per *behaviour*, not per tool.
5. Add the `live` job: detached start → probe → run → dump logs on failure → down.
6. Verify the suite can **fail**: break something on purpose and watch CI go red. This is the step
   everyone skips, and it is the only one that proves the test is connected to anything.
