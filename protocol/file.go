package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// easybus file access. Files are stored over NATS:
//   - bytes -> persistent (no-TTL) object store, keyed by `code`;
//   - metadata -> a KV bucket (FileMetaBucket) under key `f.<code>`,
//     with a dedup index under `sha.<sha256>` -> code.
//
// This is the standard the agent and every NATS member (extensions, EasyBus
// via its own bus if ever) share, so file bytes are accessible to any holder
// of a Bus — not owned by the agent.
const (
	// FileMetaBucket stores file metadata KV entries.
	FileMetaBucket = "easybus-files-meta"

	// FileMetaPrefix is the KV key prefix for a file's metadata record.
	FileMetaPrefix = "f."
	// FileShaPrefix is the KV key prefix for the sha256 -> code dedup index.
	FileShaPrefix = "sha."
)

// FileMeta is the metadata record stored under `f.<code>`.
type FileMeta struct {
	Code            string `json:"code"`
	Sha256          string `json:"sha256"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	UploaderSession string `json:"uploader_session,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// FileRecord is the resolved file: bytes + metadata.
type FileRecord struct {
	Meta FileMeta
	Data []byte
}

// FileKey returns the KV key for a file's metadata.
func FileKey(code string) string { return FileMetaPrefix + code }

// FileShaKey returns the KV key for the sha256 dedup index.
func FileShaKey(sha256 string) string { return FileShaPrefix + sha256 }

// FileStore provides NATS-backed file access. Implementers obtain it from
// `NewFileStore(bus)`. It is the single source of truth for files shared by
// the agent and extensions.
type FileStore interface {
	Put(ctx context.Context, code string, meta FileMeta, data []byte) error
	Get(ctx context.Context, code string) (*FileRecord, error)
	Stat(ctx context.Context, code string) (*FileMeta, error)
	Delete(ctx context.Context, code string) error
	// BySha returns the code of a previously stored file with the sha256 (dedup).
	BySha(ctx context.Context, sha256 string) (string, error)
}

// Bus is the transport surface the FileStore needs. It is satisfied by the
// NATS transport (and inproc/ws in tests).
type Bus interface {
	ObjectPutPersistent(ctx context.Context, name string, data []byte) error
	ObjectGetPersistent(ctx context.Context, name string) ([]byte, error)
	KVGet(ctx context.Context, bucket, key string) (string, error)
	KVPut(ctx context.Context, bucket, key, value string, ttlMs int64) error
	KVCreate(ctx context.Context, bucket, key, value string, ttlMs int64) (int64, error)
	KVDelete(ctx context.Context, bucket, key string) error
}

type fileMetaJSON struct {
	Code            string `json:"code"`
	Sha256          string `json:"sha256"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	UploaderSession string `json:"uploader_session,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// NewFileStore builds a FileStore over a NATS-backed Bus.
func NewFileStore(b Bus) FileStore {
	return &natsFileStore{b: b}
}

type natsFileStore struct {
	b Bus
}

func (s *natsFileStore) Put(ctx context.Context, code string, meta FileMeta, data []byte) error {
	if code == "" {
		return fmt.Errorf("file code required")
	}
	// Bytes first (durable object bucket), then metadata. A crash between the
	// two only loses the dedup index (harmless re-upload), never a dangling
	// index pointing at missing metadata.
	if err := s.b.ObjectPutPersistent(ctx, code, data); err != nil {
		return err
	}
	mj := fileMetaJSON{
		Code: meta.Code, Sha256: meta.Sha256, Name: meta.Name, Mime: meta.Mime,
		Size: meta.Size, UploaderSession: meta.UploaderSession, CreatedAt: meta.CreatedAt,
	}
	raw, err := json.Marshal(mj)
	if err != nil {
		return err
	}
	if err := s.b.KVPut(ctx, FileMetaBucket, FileKey(code), string(raw), 0); err != nil {
		return err
	}
	// Dedup index: hash -> code (atomic when absent; lost race means identical
	// bytes).
	if meta.Sha256 != "" {
		if _, err := s.b.KVCreate(ctx, FileMetaBucket, FileShaKey(meta.Sha256), code, 0); err != nil {
			_ = s.b.KVPut(ctx, FileMetaBucket, FileShaKey(meta.Sha256), code, 0)
		}
	}
	return nil
}

func (s *natsFileStore) Get(ctx context.Context, code string) (*FileRecord, error) {
	data, err := s.b.ObjectGetPersistent(ctx, code)
	if err != nil {
		return nil, err
	}
	meta, err := s.Stat(ctx, code)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return &FileRecord{Meta: FileMeta{Code: code}, Data: data}, nil
	}
	return &FileRecord{Meta: *meta, Data: data}, nil
}

func (s *natsFileStore) Stat(ctx context.Context, code string) (*FileMeta, error) {
	raw, err := s.b.KVGet(ctx, FileMetaBucket, FileKey(code))
	if err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var mj fileMetaJSON
	if err := json.Unmarshal([]byte(raw), &mj); err != nil {
		return nil, err
	}
	return &FileMeta{
		Code: mj.Code, Sha256: mj.Sha256, Name: mj.Name, Mime: mj.Mime,
		Size: mj.Size, UploaderSession: mj.UploaderSession, CreatedAt: mj.CreatedAt,
	}, nil
}

func (s *natsFileStore) Delete(ctx context.Context, code string) error {
	meta, err := s.Stat(ctx, code)
	if err != nil {
		return err
	}
	if meta != nil && meta.Sha256 != "" {
		_ = s.b.KVDelete(ctx, FileMetaBucket, FileShaKey(meta.Sha256))
	}
	return s.b.KVDelete(ctx, FileMetaBucket, FileKey(code))
}

func (s *natsFileStore) BySha(ctx context.Context, sha256 string) (string, error) {
	code, err := s.b.KVGet(ctx, FileMetaBucket, FileShaKey(sha256))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(code), nil
}
