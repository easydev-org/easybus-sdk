// Go-side interop agent: discovers extensions, calls tools (including the TS
// extension), and asserts the cross-language wire contract.
//
//	go run ./interop/go-agent
//
// Any assertion failure exits non-zero.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

func expect(cond bool, msg string) {
	if !cond {
		fmt.Println("[go-agent] FAIL:", msg)
		os.Exit(1)
	}
	fmt.Println("[go-agent] ok:", msg)
}

func main() {
	bus, err := nats.Connect(natsURL())
	if err != nil {
		fmt.Println("[go-agent] connect failed:", err)
		os.Exit(1)
	}
	defer bus.Close()
	ctx := context.Background()
	a := agent.New(bus)
	fmt.Println("[go-agent] connected to", natsURL())

	// 1. discover the TS extension (manifests are cached for SetConfig)
	ms, err := a.Discover(ctx, 1000)
	if err != nil {
		fmt.Println("[go-agent] discover failed:", err)
		os.Exit(1)
	}
	found := false
	for _, m := range ms {
		if m.Id == "ts-ext" {
			found = true
		}
	}
	expect(found, "discover sees ts-ext")

	// 2. tool content crosses TS->Go
	tr, err := a.CallTool(ctx, "i1", "sess-x", "ts-ext", "echo", "c1", map[string]any{"msg": "hello-from-go"})
	if err != nil {
		fmt.Println("[go-agent] echo failed:", err)
		os.Exit(1)
	}
	expect(tr.Content != "" && tr.Content == "ts-ext echo: hello-from-go", "tool echo content")

	// 3. structured data
	tr, err = a.CallTool(ctx, "i1", "sess-x", "ts-ext", "add", "c2", map[string]any{"a": 20.0, "b": 22.0})
	if err != nil {
		fmt.Println("[go-agent] add failed:", err)
		os.Exit(1)
	}
	if m, ok := tr.Data.(map[string]any); ok {
		expect(m["sum"] == 42.0, fmt.Sprintf("structured data sum=%v", m["sum"]))
	} else {
		expect(false, "structured data shape")
	}

	// 4. variable (TS resolver)
	v, ok := a.ResolveVariable(ctx, "i1", "sess-go", "ts-ext", "ts-var")
	expect(ok && v == "from-ts-ext/sess-go", "variable from ts-ext: "+v)

	// 5. call hook
	hr, err := a.CallHook(ctx, "i1", "ts-ext", "interop.before", "sess-go", map[string]any{"k": "v"})
	if err != nil {
		fmt.Println("[go-agent] hook failed:", err)
		os.Exit(1)
	}
	expect(hr.Ok, "call hook ok")
	expect(hr.Error == nil, "hook error should be nil")

	// 6. event hook: Go agent publishes, ts-ext logs on its stdout
	if err := a.PublishEventHook(ctx, "i1", "interop.event", "sess-go", map[string]any{"from": "go-agent"}); err != nil {
		fmt.Println("[go-agent] event publish failed:", err)
		os.Exit(1)
	}
	fmt.Println("[go-agent] ok: event hook published (check ts-ext stdout)")

	// 7. config: Go agent sets a knob on the TS extension
	if err := a.ServeConfig(true); err != nil {
		fmt.Println("[go-agent] ServeConfig failed:", err)
		os.Exit(1)
	}
	if err := a.SetConfig(ctx, "i1", "ts-ext", "poll-interval", float64(5), "", nil, nil); err != nil {
		fmt.Println("[go-agent] SetConfig failed:", err)
		os.Exit(1)
	}
	fmt.Println("[go-agent] ok: SetConfig poll-interval=5 delivered to ts-ext")

	fmt.Println("[go-agent] ALL INTEROP CHECKS PASSED")
}

func natsURL() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return "nats://nats.develop.svc.cluster.local:4222"
}
