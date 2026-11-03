# Conformance cases

Declarative descriptions of protocol behaviour, read by `conformance/sharedcases_test.go`.

## Why a shared file rather than tests inline

The config decoder had the same defect in Go, Python, Rust and TypeScript: each required JSON, so a
bare value (a URL, a token) was discarded entry by entry, and a structured knob was parsed into a list
that string-typed readers could not use.

Four hand-written tests would have caught it *if* someone had thought to write all four, in four
languages, describing the same thing. They were not written, and each suite passed while all four
implementations were wrong in the same way. Stating the behaviour ONCE, in data, is what made it
visible.

## Only Go reads this now

The other SDKs were deleted (see the README's "wire contract" section for why). What survives is the
format and the discipline, and both are still worth their cost with one implementation:

- **A case states the INTENT, in data.** It is a sentence about what the protocol promises, not a
  restatement of Go control flow — so a refactor that changes behaviour without changing intent fails,
  and a refactor that changes neither does not need the cases touched.
- **`kind: "constant"` still earns its place.** It asserts values two PROCESSES must agree on (the
  config bucket name, the presence key shape) — and this platform runs an agent plus four extensions,
  which is exactly the "two processes" case.

## The lock

`cases.lock` holds the SHA-256 of the case files. The test recomputes it and fails when it differs —
so editing a case without regenerating the lock is caught, rather than silently changing what the
suite asserts. The lock used to also guard against one SDK's copy drifting from another's; with one
copy left, it guards the smaller but still real risk that a case is edited in a way the lock does not
reflect.

Regenerate after an intentional change:

    ./conformance/cases/gen-lock.sh

## Format

```jsonc
{
  "id": "bare-url",              // stable name; the test names itself after this
  "why": "…",                    // the failure this pins, in one or two sentences
  "kind": "kv",                  // "kv" (seed a bucket key) | "constant"
  "kv": { "key": "t.{tenant}.{ext}.endpoint", "value": "http://…" },
  "knobs": { "endpoint": { "type": "string" } },
  "expect": { "knob": "endpoint", "value": "http://…", "valueType": "string" }
}
```

`{tenant}` and `{ext}` are substituted by the test (the suite's own fixtures), so a case never
hard-codes one suite's names.

`valueType` is `string`, `number`, `bool` or `null`, and tells the test how to compare: "42" can be a
float64 or an int depending on the decoder, and the case states the intent rather than the
representation.

`kind: "constant"` has no KV: it asserts a protocol constant that producers and consumers must agree
on, e.g. the config bucket name.
