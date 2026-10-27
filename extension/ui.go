package extension

import (
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// UISpec describes the browser UI an extension ships, if any.
//
// WHY THIS IS IN THE SDK: every extension needs the same three things — serve static files, fall
// back to index.html for client-side routes, and say where it is mounted so the frontend can set a
// base path. Written once here, an extension author adds a directory and one struct literal; written
// per repo, five copies drift and only some of them handle the fallback correctly.
//
// The UI is served UNDER A PREFIX (`Base`) and the host proxies that prefix to this extension. That
// indirection is the point: the browser talks only to the webui's origin, so an iframe embedding the
// UI is SAME-ORIGIN with the host. No CORS, no second certificate, and the host's credentials apply
// to the API calls the UI makes.
type UISpec struct {
	// Assets is the built frontend. Callers usually pass an embedded FS:
	//
	//	//go:embed all:ui/dist
	//	Assets embed.FS
	//
	// and the subdirectory it lives in, via StripPrefix.
	Assets fs.FS
	// StripPrefix is removed from the request path before lookup, e.g. "ui/dist".
	StripPrefix string
	// Base is the URL prefix the host mounts this UI at, e.g. "/ext/github".
	//
	// It is served verbatim (not stripped), because a hash-routed SPA needs its base to resolve its
	// own asset URLs correctly when the host rewrites nothing. Requests under Base serve files;
	// anything else 404s so a misrouted request is obvious rather than silently serving index.html.
	Base string
	// Title is shown in the host's breadcrumb / tab.
	Title string
}

// MountUI wraps handler so the extension's UI is served, and returns the composite handler.
//
// Paths OUTSIDE Base are left to the existing handler untouched, so probes and API routes keep
// working exactly as before. Paths INSIDE Base serve the UI: a file if one matches, otherwise
// index.html, which is what a client-side router needs.
//
// A nil or empty spec returns handler unchanged — an extension with no UI costs nothing.
func MountUI(handler http.Handler, spec *UISpec) http.Handler {
	if spec == nil || spec.Assets == nil || spec.Base == "" {
		return handler
	}
	if handler == nil {
		handler = http.NewServeMux()
	}

	base := strings.TrimSuffix(spec.Base, "/")
	files := http.FileServer(http.FS(uiFS{spec.Assets, spec.StripPrefix, base}))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != base && !strings.HasPrefix(r.URL.Path, base+"/") {
			handler.ServeHTTP(w, r)
			return
		}

		// Content the host embeds must not be framed by anything else.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")

		// The router owns everything after Base, so a request for a route (not a file) has to reach
		// index.html. Serve the file server first, and fall back only when the path has no
		// extension — "/ext/x/assets/app.js" must 404 rather than quietly return HTML, because a
		// missing asset reported as index.html is a blank page with no error anywhere.
		if p := r.URL.Path; p == base || p == base+"/" || isUIRoute(p, base) {
			r = withPath(r, "/index.html")
		} else {
			r = withPath(r, strings.TrimPrefix(p, base))
		}
		files.ServeHTTP(w, r)
	})
}

// isUIRoute reports whether p (under base) looks like a client-side route rather than a file.
//
// An extension-less last segment is a route; anything with a dot is a file that must resolve or 404.
func isUIRoute(p, base string) bool {
	rest := strings.TrimPrefix(p, base)
	return path.Ext(rest) == ""
}

// withPath clones r with a rewritten URL path, leaving the original untouched.
func withPath(r *http.Request, p string) *http.Request {
	c := r.Clone(r.Context())
	c.URL.Path = p
	c.URL.RawPath = ""
	return c
}

// uiFS maps a request path back onto the embedded tree.
//
// http.FileServer has no notion of a URL prefix, so the prefix is stripped here rather than by a
// second router: one place decides how a URL becomes a file.
type uiFS struct {
	fsys   fs.FS
	strip  string
	base   string
}

func (u uiFS) Open(name string) (fs.File, error) {
	if name == u.base {
		name = "/"
	} else {
		name = strings.TrimPrefix(name, u.base)
	}
	name = strings.TrimPrefix(path.Clean("/"+name), "/")
	if u.strip != "" {
		name = path.Join(u.strip, name)
	}
	return u.fsys.Open(name)
}
