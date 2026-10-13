package extension

import (
	"context"
	"fmt"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// IngestFileViaAgent stores bytes through the AGENT (a 1:1 `req` on
// `easybus.<tenant>.file.ingest`) and returns the canonical `file:<code>` plus the
// content type the agent DERIVED from the bytes (callers pass no mime).
//
// For extensions that own no blob backend or metadata store (no S3
// credentials, no agent DB): the agent persists the bytes to the configured
// object store and the metadata to the configured meta backend, using the SAME
// path as an in-process ingest. The tenant rides the subject/envelope, so no
// separate credential is introduced.
//
// BYTES NEVER RIDE THE MESSAGE: the bytes are first put in the transient
// object store (chunked by the transport, so any size is fine) and only the
// object reference travels in the request.
func IngestFileViaAgent(
	ctx context.Context,
	b bus.Bus,
	tenant, name string,
	data []byte,
	sessionName string,
) (code string, mime string, err error) {
	object := protocol.NewID() + ".ingest"
	if err := b.ObjectPut(ctx, protocol.TenantObjectName(tenant, object), data); err != nil {
		return "", "", fmt.Errorf("file ingest object put: %w", err)
	}
	req := protocol.FileIngestRequest{
		Name:        name,
		Object:      object,
		SessionName: sessionName,
	}
	opts := bus.RequestOpts{Tenant: tenant}
	if sessionName != "" {
		opts.SessionName = sessionName
	}
	env, err := b.Request(ctx, protocol.ChFileIngest(tenant), req, opts)
	if err != nil {
		return "", "", fmt.Errorf("file ingest request: %w", err)
	}
	var res protocol.FileIngestResponse
	if !protocol.Coerce(env.Payload, &res) {
		return "", "", fmt.Errorf("file ingest: malformed response")
	}
	if !res.Ok {
		if res.Error != nil && res.Error.Message != "" {
			return "", "", fmt.Errorf("file ingest: %s", res.Error.Message)
		}
		return "", "", fmt.Errorf("file ingest failed")
	}
	return res.Code, res.Mime, nil
}

// GetFileViaAgent fetches a stored file's bytes + metadata through the agent
// (a 1:1 `req` on `easybus.<tenant>.file.get`). Returns (nil, nil, nil) when the
// file is absent. The agent writes the bytes to the transient object store and
// returns the reference, so the bytes never ride the message.
func GetFileViaAgent(
	ctx context.Context,
	b bus.Bus,
	tenant, code string,
) (*protocol.FileMetaWire, []byte, error) {
	env, err := b.Request(
		ctx,
		protocol.ChFileGet(tenant),
		protocol.FileGetRequest{Code: code},
		bus.RequestOpts{Tenant: tenant},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("file get request: %w", err)
	}
	var res protocol.FileGetResponse
	if !protocol.Coerce(env.Payload, &res) {
		return nil, nil, fmt.Errorf("file get: malformed response")
	}
	if !res.Ok || res.Meta == nil || res.Object == "" {
		return nil, nil, nil
	}
	data, err := b.ObjectGet(ctx, protocol.TenantObjectName(tenant, res.Object))
	if err != nil {
		return nil, nil, fmt.Errorf("file get object: %w", err)
	}
	return res.Meta, data, nil
}
