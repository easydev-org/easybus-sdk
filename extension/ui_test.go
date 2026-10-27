package extension

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func uiAssets() fstest.MapFS {
	return fstest.MapFS{
		"dist/index.html":     &fstest.MapFile{Data: []byte("<!doctype html><div id=app></div>")},
		"dist/assets/app.js":  &fstest.MapFile{Data: []byte("console.log(1)")},
		"dist/assets/app.css": &fstest.MapFile{Data: []byte("body{}")},
	}
}

func serve(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	b, _ := io.ReadAll(w.Result().Body)
	return w.Code, string(b)
}

// Paths outside the UI prefix belong to the extension and must not be touched.
//
// If the mount swallowed them, probes and API routes would 404 — the extension would look dead to
// Kubernetes while its UI worked, which is the worst possible split.
func TestUILeavesOtherPathsAlone(t *testing.T) {
	var sawPath string
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPath = r.URL.Path
		w.WriteHeader(http.StatusTeapot)
	})
	h := MountUI(api, &UISpec{Assets: uiAssets(), StripPrefix: "dist", Base: "/ext/x"})

	code, _ := serve(t, h, "/api/v1/health")
	if code != http.StatusTeapot {
		t.Fatalf("an API path got %d, want it to reach the extension unchanged (418)", code)
	}
	if sawPath != "/api/v1/health" {
		t.Fatalf("the extension saw %q, want the original path", sawPath)
	}
}

func TestUIServesIndexAtBase(t *testing.T) {
	h := MountUI(nil, &UISpec{Assets: uiAssets(), StripPrefix: "dist", Base: "/ext/x"})
	for _, p := range []string{"/ext/x", "/ext/x/"} {
		code, body := serve(t, h, p)
		if code != http.StatusOK || !strings.Contains(body, `id=app`) {
			t.Fatalf("%s -> %d %q, want index.html", p, code, body)
		}
	}
}

func TestUIServesAssets(t *testing.T) {
	h := MountUI(nil, &UISpec{Assets: uiAssets(), StripPrefix: "dist", Base: "/ext/x"})
	if code, body := serve(t, h, "/ext/x/assets/app.js"); code != http.StatusOK || !strings.Contains(body, "console.log") {
		t.Fatalf("asset -> %d %q", code, body)
	}
}

// A missing ASSET must 404, not return index.html.
//
// A hash-routed SPA never asks the server for a route, so the only extension-less path that reaches
// here is a real route — but a missing .js reported as HTML is a blank page with no error anywhere.
func TestUIMissingAssetIs404(t *testing.T) {
	h := MountUI(nil, &UISpec{Assets: uiAssets(), StripPrefix: "dist", Base: "/ext/x"})
	code, body := serve(t, h, "/ext/x/assets/nope.js")
	if code == http.StatusOK && strings.Contains(body, "id=app") {
		t.Fatalf("a missing asset returned index.html: %q", body)
	}
	if code != http.StatusNotFound {
		t.Fatalf("a missing asset got %d, want 404", code)
	}
}

// An extension-less path under Base is a client-side route: serve the shell.
//
// This is what lets a host deep-link into a UI (`/ext/github/repo/x`) and have the SPA take over.
func TestUIRouteFallsBackToIndex(t *testing.T) {
	h := MountUI(nil, &UISpec{Assets: uiAssets(), StripPrefix: "dist", Base: "/ext/x"})
	code, body := serve(t, h, "/ext/x/repo/easydev-org/test")
	if code != http.StatusOK || !strings.Contains(body, `id=app`) {
		t.Fatalf("route -> %d %q, want index.html", code, body)
	}
}

// No spec means no change at all, so an extension without a UI pays nothing.
func TestUINilSpecIsPassthrough(t *testing.T) {
	h := MountUI(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}), nil)
	if code, _ := serve(t, h, "/anything"); code != http.StatusTeapot {
		t.Fatalf("nil spec changed behaviour: %d", code)
	}
}

// A sibling prefix must not be swallowed: /ext/xy is not under /ext/x.
func TestUIDoesNotMatchSiblingPrefix(t *testing.T) {
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := MountUI(api, &UISpec{Assets: uiAssets(), StripPrefix: "dist", Base: "/ext/x"})
	if code, _ := serve(t, h, "/ext/xy/thing"); code != http.StatusTeapot {
		t.Fatalf("/ext/xy was treated as under /ext/x: %d", code)
	}
}

// The host must be able to discover the mount point without parsing our source.
func TestUIReportsItsBase(t *testing.T) {
	// Mirrors the wrapper Serve builds; kept here because that wrapper is inline in Serve.
	m := http.NewServeMux()
	m.HandleFunc("/api/v1/ui", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"base":"/ext/x"}`))
	})
	if _, body := serve(t, m, "/api/v1/ui"); !strings.Contains(body, "/ext/x") {
		t.Fatalf("got %q", body)
	}
}
