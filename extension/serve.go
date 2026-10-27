package extension

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Serve is the one-stop main() body of an extension: it registers the
// extension on the bus, serves the HTTP surface, starts optional background
// workers, and blocks until SIGINT/SIGTERM — then shuts everything down in
// order (HTTP → extension subscriptions → bus).
//
//	b := <transport connect>
//	ext := easybus.NewExtension(b, cfg)   // or manifest.BuildConfig(...)
//	easybusextension.Serve(ext, easybusextension.ServeOptions{Handler: r, Run: run})
type ServeOptions struct {
	// Handler serves the extension's HTTP surface (k8s probes, ops API).
	// When nil a minimal handler is mounted: GET /api/v1/health and
	// GET /api/v1/version.
	Handler http.Handler
	// Run starts background workers after Serve; its context is canceled on
	// shutdown before the extension and bus close.
	Run func(ctx context.Context, ext *Extension)
	// Port overrides EASYBUS_PORT (default 8080) when non-empty.
	Port string
	// UI, when set, is served from this extension's HTTP surface under UISpec.Base.
	//
	// It is wrapped AROUND Handler rather than merged into it, so an extension never has to know
	// which paths the UI claims: paths outside Base reach Handler exactly as before. See MountUI.
	UI *UISpec
}

// Serve blocks until SIGINT/SIGTERM or a hard error, then shuts everything
// down in order (HTTP → extension subscriptions → bus).
func Serve(ext *Extension, opts ServeOptions) error {
	port := opts.Port
	if port == "" {
		port = os.Getenv("EASYBUS_PORT")
	}
	if port == "" {
		port = "8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := ext.Serve(ctx); err != nil {
		return err
	}
	if opts.Run != nil {
		go opts.Run(ctx, ext)
	}

	handler := opts.Handler
	if handler == nil {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "name": ext.cfg.ID})
		})
		mux.HandleFunc("/api/v1/version", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": ext.cfg.ID, "version": ext.cfg.Version})
		})
		handler = mux
	}

	// The UI claims only its own prefix; everything else (probes, API) is untouched.
	handler = MountUI(handler, opts.UI)

	// A host that embeds the UI needs to know the mount point without parsing our source.
	if opts.UI != nil && opts.UI.Base != "" {
		inner := handler
		m := http.NewServeMux()
		m.HandleFunc("/api/v1/ui", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"base": opts.UI.Base, "title": opts.UI.Title, "id": ext.cfg.ID,
			})
		})
		m.Handle("/", inner)
		handler = m
	}
	srv := &http.Server{Addr: ":" + port, Handler: handler}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		_ = ext.Close()
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = ext.Close()
	return nil
}
