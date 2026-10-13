package protocol

// File RPCs (agent-served). A DB-less, store-less extension (no S3
// credentials, no agent DB) cannot mint a canonical `file:<code>` on its own,
// so it asks the AGENT to persist the bytes + metadata via the SAME path as an
// in-process ingest. The tenant rides the subject (`easybus.<tenant>.file.ingest`)
// and the envelope, so no separate credential is introduced.
const (
	// FileIngestWildcard is the cross-tenant consume wildcard for file.ingest.
	FileIngestWildcard = "easybus.*.file.ingest"
	// FileGetWildcard is the cross-tenant consume wildcard for file.get.
	FileGetWildcard = "easybus.*.file.get"
)

// ChFileIngest routes a file-ingest request for one tenant.
func ChFileIngest(tenant string) string {
	return TenantPrefix(tenant) + "file.ingest"
}

// ChFileGet routes a file-get request for one tenant.
func ChFileGet(tenant string) string {
	return TenantPrefix(tenant) + "file.get"
}

// FileIngestRequest asks the agent to store bytes and return a file code.
//
// BYTES NEVER RIDE THE MESSAGE: the caller first stores the bytes in the
// transient object store under TenantObjectName(tenant, object) (the transport
// chunks them, so any size works and the broker max_payload is irrelevant),
// then sends only that object reference here.
type FileIngestRequest struct {
	Code        string `json:"code,omitempty"`
	Name        string `json:"name"`
	Object      string `json:"object"`
	SessionName string `json:"session_name,omitempty"`
}

// FileMetaWire mirrors the agent's file metadata record on the wire.
//
// The optional media fields are SERVER-derived (agent-side ffprobe/ffmpeg):
// width/height (px), duration_ms, thumb_code (the code of a separate
// content-addressed thumbnail file) and thumbhash (base64 ThumbHash). They are
// absent for non-media files and for files stored before the feature existed.
type FileMetaWire struct {
	Code            string `json:"code"`
	Sha256          string `json:"sha256"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	UploaderSession string `json:"uploader_session,omitempty"`
	CreatedAt       string `json:"created_at"`
	Width           *int32 `json:"width,omitempty"`
	Height          *int32 `json:"height,omitempty"`
	DurationMs      *int64 `json:"duration_ms,omitempty"`
	ThumbCode       string `json:"thumb_code,omitempty"`
	Thumbhash       string `json:"thumbhash,omitempty"`
}

// FileErrorPayload carries a standard error code + message.
type FileErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// FileIngestResponse is the reply to a FileIngestRequest. Mime is the content
// type the agent DERIVED from the bytes (the caller supplies none).
type FileIngestResponse struct {
	Ok    bool              `json:"ok"`
	Code  string            `json:"code,omitempty"`
	Mime  string            `json:"mime,omitempty"`
	Error *FileErrorPayload `json:"error,omitempty"`
}

// FileGetRequest asks the agent for a stored file's bytes + metadata.
type FileGetRequest struct {
	Code string `json:"code"`
}

// FileGetResponse is the reply to a FileGetRequest. As with ingest, bytes never
// ride the message: the agent writes them to the transient object store and
// returns only the object reference; the caller reads it via ObjectGet.
type FileGetResponse struct {
	Ok     bool              `json:"ok"`
	Meta   *FileMetaWire     `json:"meta,omitempty"`
	Object string            `json:"object,omitempty"`
	Error  *FileErrorPayload `json:"error,omitempty"`
}
