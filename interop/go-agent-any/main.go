// A generic Go interop agent: discovers the extension named by EXT_ID and
// asserts the cross-language wire contract against it. Used to drive the TS /
// Python / Rust interop extensions (their own agents are the mirror).
//
//	EXT_ID=py-ext go run ./interop/go-agent-any
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

func expect(cond bool, msg string) {
	if !cond {
		fmt.Println("[go-agent-any] FAIL:", msg)
		os.Exit(1)
	}
	fmt.Println("[go-agent-any] ok:", msg)
}

func main() {
	extID := os.Getenv("EXT_ID")
	if extID == "" {
		extID = "py-ext"
	}
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	bus, err := nats.Connect(url)
	if err != nil {
		fmt.Println("[go-agent-any] connect failed:", err)
		os.Exit(1)
	}
	defer bus.Close()
	ctx := context.Background()
	a := agent.New(bus)
	fmt.Println("[go-agent-any] connected to", url, "ext=", extID)

	ms, err := a.Discover(ctx, 1500)
	if err != nil {
		fmt.Println("[go-agent-any] discover failed:", err)
		os.Exit(1)
	}
	found := false
	for _, m := range ms {
		if m.Id == extID {
			found = true
		}
	}
	expect(found, "discover sees "+extID)

	tr, err := a.CallTool(ctx, "i1", "sess-x", extID, "echo", "c1", map[string]any{"msg": "hello-from-go"})
	if err != nil {
		fmt.Println("[go-agent-any] echo failed:", err)
		os.Exit(1)
	}
	expect(tr.Content == extID+" echo: hello-from-go", "tool echo content")

	tr, err = a.CallTool(ctx, "i1", "sess-x", extID, "add", "c2", map[string]any{"a": 20.0, "b": 22.0})
	if err != nil {
		fmt.Println("[go-agent-any] add failed:", err)
		os.Exit(1)
	}
	if m, ok := tr.Data.(map[string]any); ok {
		expect(fmt.Sprintf("%v", m["sum"]) == "42", "structured data sum")
	} else {
		expect(false, "structured data shape")
	}

	tr, err = a.CallTool(ctx, "i1", "sess-x", extID, "fail", "c3", map[string]any{})
	if err != nil {
		fmt.Println("[go-agent-any] fail failed:", err)
		os.Exit(1)
	}
	expect(tr.Error != nil && strings.Contains(tr.Error.Message, "deliberate failure"), "business error mapping")

	fmt.Println("[go-agent-any] ALL INTEROP CHECKS PASSED")
}
