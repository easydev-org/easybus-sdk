# Shared conformance cases

Language-neutral descriptions of behaviour every SDK must implement identically.
Each SDK reads every `*.json` in this directory and asserts it, so a divergence
fails in the repository that drifted instead of surfacing as a bug in the field.

## Why a shared file rather than a test per language

The config decoder had the same defect in Go, Python, Rust and TypeScript: each
required JSON, so a bare value (a URL, a token) was discarded entry by entry, and a
structured knob was parsed into a list that string-typed readers could not use.

Four hand-written tests would have caught it *if* someone had thought to write all
four, in four languages, describing the same thing. They were not written, and each
suite passed while all four implementations were wrong in the same way. The
behaviour has to be stated ONCE, in a form every SDK consumes.

## Keeping the copies in step

This directory is the authority. Each SDK carries a copy plus `cases.lock`
containing the SHA-256 of the case files; the SDK's test recomputes the hash and
fails when it differs. Editing a copy without updating the lock — or updating the
lock from a copy that differs from this one — is what the check exists to prevent.

Regenerate the lock after an intentional change:

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

`{tenant}` and `{ext}` are substituted by the test (the SDK's own fixtures), so a
case never hard-codes one suite's names.

`valueType` is `string`, `number`, `bool` or `null`, and tells the test how to
compare: implementations represent "42" differently (float64, f64, number), and the
case states the intent rather than the representation.

`kind: "constant"` has no KV: it asserts a protocol constant that two processes must
agree on, e.g. the config bucket name.
