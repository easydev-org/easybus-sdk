package bus

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Options configures an S3-compatible ObjectStore (AWS S3, MinIO, Ceph, …).
type S3Options struct {
	Bucket          string
	Region          string
	Endpoint        string // empty = AWS S3
	AccessKeyID     string
	SecretAccessKey string
	ForcePathStyle  bool
	Prefix          string
}

// S3ObjectStore stores objects in an S3-compatible bucket. Distinct classes
// use distinct key prefixes; a deployment selects it to keep DURABLE file
// bytes (and, when chosen, transient objects) out of NATS.
type S3ObjectStore struct {
	client *s3.Client
	bucket string
	prefix string
}

var _ ObjectStore = (*S3ObjectStore)(nil)

// NewS3ObjectStore builds the store (must be closed via Close when done).
func NewS3ObjectStore(ctx context.Context, o S3Options) (*S3ObjectStore, error) {
	if o.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	region := o.Region
	if region == "" {
		region = "us-east-1"
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if o.AccessKeyID != "" && o.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(o.AccessKeyID, o.SecretAccessKey, ""),
		))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(svc *s3.Options) {
		if o.Endpoint != "" {
			svc.BaseEndpoint = aws.String(o.Endpoint)
			svc.UsePathStyle = o.ForcePathStyle
		}
	})
	return &S3ObjectStore{
		client: client,
		bucket: o.Bucket,
		prefix: strings.TrimRight(o.Prefix, "/"),
	}, nil
}

func (s *S3ObjectStore) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return s.prefix + "/" + name
}

func (s *S3ObjectStore) ObjectPut(ctx context.Context, name string, data []byte) error {
	return s.ObjectPutPersistent(ctx, name, data)
}

func (s *S3ObjectStore) ObjectPutPersistent(ctx context.Context, name string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(name)),
		Body:   strings.NewReader(string(data)),
	})
	return err
}

func (s *S3ObjectStore) ObjectGet(ctx context.Context, name string) ([]byte, error) {
	return s.ObjectGetPersistent(ctx, name)
}

func (s *S3ObjectStore) ObjectGetPersistent(ctx context.Context, name string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(name)),
	})
	if err != nil {
		// A missing key is a normal "nil bytes"; only surface real failures.
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil
		}
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}
