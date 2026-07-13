package goutils

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/apex/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// maxPresignedURLTTL is the maximum TTL S3 allows for a presigned URL (7 days).
// Requests with a longer expiry are rejected by the object store, so reject
// them up front with a clearer error.
const maxPresignedURLTTL = 7 * 24 * time.Hour

// validatePresignTTL validates a presigned URL TTL against S3's limits.
func validatePresignTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return NewBadInputError(
			fmt.Sprintf("presigned URL TTL must be positive, got %s", ttl), nil, true,
		)
	}
	if ttl > maxPresignedURLTTL {
		return NewBadInputError(
			fmt.Sprintf(
				"presigned URL TTL %s exceeds the maximum of %s", ttl, maxPresignedURLTTL,
			), nil, true,
		)
	}
	return nil
}

// S3ObjectStat S3 object stats
type S3ObjectStat struct {
	// MIMEType object MIME type
	MIMEType string
	// Size object file size
	Size int64
	// CheckSum object SHA-256 checksum, base64-encoded. May be empty for objects
	// that were not uploaded with a SHA-256 checksum (e.g. by other tools, or via
	// multipart uploads that store a composite checksum). Objects uploaded through
	// PutObject always carry one.
	CheckSum string
}

// S3Credentials S3 credentials
type S3Credentials struct {
	// AccessKey object store access key
	AccessKey string `json:"access_id" validate:"required"`
	// SecretAccessKey object store secret access key
	SecretAccessKey string `json:"secret_id" validate:"required"`
}

// S3Config S3 object store config
type S3Config struct {
	// ServerEndpoint S3 server endpoint
	ServerEndpoint string `json:"endpoint" validate:"required"`
	// UseTLS whether to TLS when connecting
	UseTLS bool `json:"useTLS"`
	// Region optional S3 region. When set, it is used as the client region (which,
	// among other things, is the region newly created buckets are placed in). When
	// nil, the region is left for the server / minio to resolve automatically.
	Region *string `json:"region,omitempty" validate:"omitempty"`
	// Creds S3 credentials
	Creds S3Credentials `json:"creds,omitempty" validate:"omitempty"`
}

// S3Client client for interacting with S3
type S3Client interface {
	/*
		ListBuckets get a list of available buckets at the server

			@param ctx context.Context - execution context
			@returns list of bucket names
	*/
	ListBuckets(ctx context.Context) ([]string, error)

	/*
		ListObjects get a list of objects in a bucket

			@param ctx context.Context - execution context
			@param bucket string - the bucket name
			@param prefix *string - optionally, specify the object prefix to filter on
			@param startingKey *string - optionally, list keys lexically after this one
			       (exclusive: the given key itself is not returned). S3 paginates by
			       key, not offset, so pass the last key of a previous page to continue.
			@param maxKeys *int - optionally, cap the total number of keys returned. If
			       nil, all matching keys are returned.
			@return list of bucket objects
	*/
	ListObjects(
		ctx context.Context,
		bucket string,
		prefix *string,
		startingKey *string,
		maxKeys *int,
	) ([]string, error)

	/*
		CreateBucket create a bucket

			@param ctx context.Context - execution context
			@param bucket string - new bucket name
	*/
	CreateBucket(ctx context.Context, bucket string) error

	/*
		DeleteBucket delete a bucket

			@param ctx context.Context - execution context
			@param bucket string - new bucket name
	*/
	DeleteBucket(ctx context.Context, bucket string) error

	/*
		PutObject put a new object into a bucket

		A SHA-256 checksum is always computed and stored with the object, so it is
		available later via GetObjectStat.

			@param ctx context.Context - execution context
			@param bucketName string - target bucket name
			@param objectKey string - target object name within the bucket
			@param data io.Reader - data reader to the content
			@param expectedSize int64 - expected object size, or -1, if unknown
			@param contentType *string - if specified, the object MIME type; otherwise
			       the object store defaults to "application/octet-stream".
	*/
	PutObject(
		ctx context.Context,
		bucket string,
		objectKey string,
		data io.Reader,
		expectedSize int64,
		contentType *string,
	) error

	/*
		CopyObject copy an object from one bucket/key to another. The source and
		destination buckets may be the same or different. The copy is performed
		server-side and this call blocks until it completes.

		When newMIMEType is set, the destination's Content-Type is rewritten to
		that value. NOTE: this uses a single-part server-side copy, which S3 caps
		at 5GiB; larger objects will fail.

			@param ctx context.Context - execution context
			@param srcBucket string - the source bucket
			@param srcKey string - the source object key
			@param dstBucket string - the destination bucket
			@param dstKey string - the destination object key
			@param newMIMEType *string - if specified, change the destination object MIME type.
	*/
	CopyObject(
		ctx context.Context,
		srcBucket string,
		srcKey string,
		dstBucket string,
		dstKey string,
		newMIMEType *string,
	) error

	/*
		GetObject get an object from a bucket

		The function performs a object stat read first before returning the object reader. However,
		because the object is not read here, it is not guaranteed that object reader will not
		encounter errors later on.

			@param ctx context.Context - execution context
			@param bucketName string - target bucket name
			@param objectKey string - target object name within the bucket
			@returns basic object stat and associated content reader
	*/
	GetObject(
		ctx context.Context, bucket string, objectKey string,
	) (S3ObjectStat, S3ObjectReader, error)

	/*
		GetObjectStat get object file stats

		The returned CheckSum may be empty; see S3ObjectStat.CheckSum.

			@param ctx context.Context - execution context
			@param bucketName string - target bucket name
			@param objectKey string - target object name within the bucket
			@returns object file stats
	*/
	GetObjectStat(
		ctx context.Context, bucket string, objectKey string,
	) (S3ObjectStat, error)

	/*
		DeleteObject delete an object from a bucket

			@param ctxt context.Context - execution context
			@param bucketName string - target bucket name
			@param objectKey string - target object name within the bucket
	*/
	DeleteObject(ctx context.Context, bucket, objectKey string) error

	/*
		DeleteObjects delete a group of objects from a bucket

			@param ctxt context.Context - execution context
			@param bucketName string - target bucket name
			@param objectKeys []string - target object names within the bucket
			@returns map of per-object-key deletion errors (empty if all succeeded)
			@returns error a function-level error, e.g. if the context is cancelled
			         before the bulk delete completes

		On cancellation both returns may be non-nil: the map holds the per-object
		failures reported before the delete stopped, and the error signals the
		cancellation. When the error is non-nil the map is not exhaustive, so a
		key's absence does not confirm it was deleted.
	*/
	DeleteObjects(
		ctx context.Context, bucketName string, objectKeys []string,
	) (map[string]error, error)

	/*
		GeneratePresignedGetURL generate presigned GET URL from S3 server for a particular
		bucket and object key.

			@param ctx context.Context - execution context
			@param bucketName string - target bucket name
			@param objectKey string - target object key
			@param ttl time.Duration - TTL for the pre-signed URL
			@returns presigned URL
	*/
	GeneratePresignedGetURL(
		ctx context.Context, bucketName string, objectKey string, ttl time.Duration,
	) (*url.URL, error)

	/*
		GeneratePresignedPutURL generate presigned PUT URL from S3 server for a particular
		bucket and object key.

		The returned URL binds the upload to an exact `Content-Length` and a base64-encoded
		SHA-256 of the body (sent as the `x-amz-checksum-sha256` header). The HTTP client
		using this URL must send both headers with these exact values, or the object store
		will reject the request with a signature or checksum error.

			@param ctx context.Context - execution context
			@param bucketName string - target bucket name
			@param objectKey string - target object key
			@param objectSize int64 - exact size in bytes of the object the caller will upload
			@param sha256Sum string - base64-encoded SHA-256 of the object content
			@param ttl time.Duration - TTL for the pre-signed URL
			@returns presigned URL
	*/
	GeneratePresignedPutURL(
		ctx context.Context,
		bucketName string,
		objectKey string,
		objectSize int64,
		sha256Sum string,
		ttl time.Duration,
	) (*url.URL, error)
}

// s3ClientImpl implements S3Client
type s3ClientImpl struct {
	Component
	s3 *minio.Client
}

/*
NewS3Client define new S3 operation client

	@param config S3Config - S3 client config
	@returns new client
*/
func NewS3Client(config S3Config) (S3Client, error) {
	logTags := log.Fields{
		"module": "common", "component": "s3-client", "instance": config.ServerEndpoint,
	}

	// Define the core minio client
	options := minio.Options{
		Creds:  credentials.NewStaticV4(config.Creds.AccessKey, config.Creds.SecretAccessKey, ""),
		Secure: config.UseTLS,
		// TrailingHeaders is required for PutObject to compute and send a
		// SHA-256 checksum (see PutObject).
		TrailingHeaders: true,
	}
	// Only set the region when provided; otherwise leave it empty so minio
	// resolves it automatically.
	if config.Region != nil {
		options.Region = *config.Region
	}
	client, err := minio.New(config.ServerEndpoint, &options)
	if err != nil {
		return nil, NewObjectStoreError(
			fmt.Sprintf("failed to create S3 client to '%s'", config.ServerEndpoint), err, true,
		)
	}

	return &s3ClientImpl{
		Component: Component{
			LogTags: logTags,
			LogTagModifiers: []LogMetadataModifier{
				ModifyLogMetadataByRestRequestParam,
			},
		}, s3: client,
	}, nil
}

/*
ListBuckets get a list of available buckets at the server

	@param ctx context.Context - execution context
	@returns list of bucket names
*/
func (s *s3ClientImpl) ListBuckets(ctx context.Context) ([]string, error) {
	buckets, err := s.s3.ListBuckets(ctx)
	if err != nil {
		return nil, NewObjectStoreError("failed to list buckets", err, true)
	}
	result := []string{}
	for _, bucket := range buckets {
		result = append(result, bucket.Name)
	}
	return result, nil
}

/*
ListObjects get a list of objects in a bucket

	@param ctx context.Context - execution context
	@param bucket string - the bucket name
	@param prefix *string - optionally, specify the object prefix to filter on
	@param startingKey *string - optionally, list keys lexically after this one
	       (exclusive: the given key itself is not returned). S3 paginates by
	       key, not offset, so pass the last key of a previous page to continue.
	@param maxKeys *int - optionally, cap the total number of keys returned. If
	       nil, all matching keys are returned.
	@return list of bucket objects
*/
func (s *s3ClientImpl) ListObjects(
	ctx context.Context,
	bucket string,
	prefix *string,
	startingKey *string,
	maxKeys *int,
) ([]string, error) {
	options := minio.ListObjectsOptions{
		Recursive: true,
	}
	if prefix != nil {
		options.Prefix = *prefix
	}
	if startingKey != nil {
		options.StartAfter = *startingKey
	}
	if maxKeys != nil {
		// Bound the per-request batch too so the server does not over-fetch on
		// the final page; the total is still capped by the loop below.
		options.MaxKeys = *maxKeys
	}

	// Derive a cancelable context so we can stop pagination the moment we hit the
	// cap or an error. Cancelling makes minio's lister goroutine stop issuing
	// further list requests rather than draining the whole bucket.
	listCtx, cancelList := context.WithCancel(ctx)
	defer cancelList()

	objReadCh := s.s3.ListObjects(listCtx, bucket, options)

	// Drain the channel on exit so the minio lister goroutine can observe the
	// cancellation and return instead of blocking on a send. On the happy path
	// the channel is already closed, so this is a no-op.
	defer func() {
		//revive:disable-next-line:empty-block // draining the channel is the work
		for range objReadCh {
		}
	}()

	result := []string{}
	for objInfo := range objReadCh {
		if objInfo.Err != nil {
			message := fmt.Sprintf("failed to list object in bucket '%s'", bucket)
			if prefix != nil {
				message = fmt.Sprintf("failed to list object in bucket '%s/%s'", bucket, *prefix)
			}
			return nil, NewObjectStoreError(message, objInfo.Err, true)
		}
		result = append(result, objInfo.Key)
		if maxKeys != nil && len(result) >= *maxKeys {
			cancelList()
			break
		}
	}

	return result, nil
}

/*
CreateBucket create a bucket

This is idempotent: if the bucket already exists and is owned by this account,
it returns nil. If the bucket exists but is owned by someone else, it still
returns an error.

	@param ctx context.Context - execution context
	@param bucket string - new bucket name
*/
func (s *s3ClientImpl) CreateBucket(ctx context.Context, bucket string) error {
	if err := s.s3.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		// The bucket already existing and being owned by us is not an error for
		// an idempotent create.
		if minio.ToErrorResponse(err).Code == "BucketAlreadyOwnedByYou" {
			return nil
		}
		return NewObjectStoreError(
			fmt.Sprintf("failed to create bucket '%s'", bucket), err, true,
		)
	}
	return nil
}

/*
DeleteBucket delete a bucket

	@param ctx context.Context - execution context
	@param bucket string - new bucket name
*/
func (s *s3ClientImpl) DeleteBucket(ctx context.Context, bucket string) error {
	if err := s.s3.RemoveBucket(ctx, bucket); err != nil {
		return NewObjectStoreError(
			fmt.Sprintf("failed to delete bucket '%s'", bucket), err, true,
		)
	}
	return nil
}

/*
PutObject put a new object into a bucket

A SHA-256 checksum is always computed and stored with the object, so it is
available later via GetObjectStat.

	@param ctx context.Context - execution context
	@param bucketName string - target bucket name
	@param objectKey string - target object name within the bucket
	@param data io.Reader - data reader to the content
	@param expectedSize int64 - expected object size, or -1, if unknown
	@param contentType *string - if specified, the object MIME type; otherwise
	       the object store defaults to "application/octet-stream".
*/
func (s *s3ClientImpl) PutObject(
	ctx context.Context,
	bucket string,
	objectKey string,
	data io.Reader,
	expectedSize int64,
	contentType *string,
) error {
	options := minio.PutObjectOptions{Checksum: minio.ChecksumSHA256}
	if contentType != nil {
		options.ContentType = *contentType
	}

	_, err := s.s3.PutObject(ctx, bucket, objectKey, data, expectedSize, options)

	if err != nil {
		return NewObjectStoreError(
			fmt.Sprintf("failed to upload '%s/%s'", bucket, objectKey), err, true,
		)
	}

	return nil
}

/*
CopyObject copy an object from one bucket/key to another. The source and
destination buckets may be the same or different. The copy is performed
server-side and this call blocks until it completes.

When newMIMEType is set, the destination's Content-Type is rewritten to that
value (via a metadata-replace copy); this is the only way to change an object's
MIME type in S3. This underpins the staging flow where an uploaded object's MIME
type is detected server-side and then applied while promoting it to its final
location.

NOTE: this uses a single-part server-side copy, which S3 caps at 5GiB. Objects
larger than 5GiB will fail and would require a multipart copy (ComposeObject),
which cannot rewrite the Content-Type and so is not used here.

	@param ctx context.Context - execution context
	@param srcBucket string - the source bucket
	@param srcKey string - the source object key
	@param dstBucket string - the destination bucket
	@param dstKey string - the destination object key
	@param newMIMEType *string - if specified, change the destination object MIME type.
*/
func (s *s3ClientImpl) CopyObject(
	ctx context.Context,
	srcBucket string,
	srcKey string,
	dstBucket string,
	dstKey string,
	newMIMEType *string,
) error {
	srcObjParam := minio.CopySrcOptions{Bucket: srcBucket, Object: srcKey}

	dstObjParam := minio.CopyDestOptions{
		Bucket:          dstBucket,
		Object:          dstKey,
		ReplaceMetadata: true,
		UserMetadata:    map[string]string{},
		ReplaceTags:     true,
		UserTags:        map[string]string{},
	}
	if newMIMEType != nil {
		dstObjParam.ContentType = *newMIMEType
	}

	// Copy the object. CopyObject sends the metadata-replace directive together
	// with the new Content-Type, which is required for the MIME-rewrite flow.
	if _, err := s.s3.CopyObject(ctx, dstObjParam, srcObjParam); err != nil {
		return NewObjectStoreError(
			fmt.Sprintf(
				"failed to copy object s3://%s/%s to s3://%s/%s",
				srcBucket, srcKey, dstBucket, dstKey,
			), err, true,
		)
	}

	return nil
}

// S3ObjectReader S3 object content reader
type S3ObjectReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

/*
GetObject get an object from a bucket

The function performs a object stat read first before returning the object reader. However,
because the object is not read here, it is not guaranteed that object reader will not
encounter errors later on.

	@param ctx context.Context - execution context
	@param bucketName string - target bucket name
	@param objectKey string - target object name within the bucket
	@returns basic object stat and associated content reader
*/
func (s *s3ClientImpl) GetObject(
	ctx context.Context, bucket string, objectKey string,
) (S3ObjectStat, S3ObjectReader, error) {
	// Preflight check that the object exist - still suspectable to TOCTOU but better than nothing
	objectStat, err := s.GetObjectStat(ctx, bucket, objectKey)
	if err != nil {
		return S3ObjectStat{}, nil, err
	}
	objectHandle, err := s.s3.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return S3ObjectStat{}, nil, NewObjectStoreError(
			fmt.Sprintf("failed to get '%s/%s'", bucket, objectKey), err, true,
		)
	}
	return objectStat, objectHandle, nil
}

/*
GetObjectStat get object file stats

The returned CheckSum may be empty; see S3ObjectStat.CheckSum.

	@param ctx context.Context - execution context
	@param bucketName string - target bucket name
	@param objectKey string - target object name within the bucket
	@returns object file stats
*/
func (s *s3ClientImpl) GetObjectStat(
	ctx context.Context, bucket string, objectKey string,
) (S3ObjectStat, error) {
	info, err := s.s3.StatObject(ctx, bucket, objectKey, minio.GetObjectOptions{Checksum: true})
	if err != nil {
		return S3ObjectStat{}, NewObjectStoreError(
			fmt.Sprintf("failed to get object s3://%s/%s stats", bucket, objectKey), err, true,
		)
	}
	return S3ObjectStat{
		MIMEType: info.ContentType, Size: info.Size, CheckSum: info.ChecksumSHA256,
	}, nil
}

/*
DeleteObject delete an object from a bucket

	@param ctxt context.Context - execution context
	@param bucketName string - target bucket name
	@param objectKey string - target object name within the bucket
*/
func (s *s3ClientImpl) DeleteObject(ctx context.Context, bucket, objectKey string) error {
	if err := s.s3.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{}); err != nil {
		return NewObjectStoreError(
			fmt.Sprintf("failed to delete '%s/%s'", bucket, objectKey), err, true,
		)
	}
	return nil
}

/*
DeleteObjects delete a group of objects from a bucket

	@param ctxt context.Context - execution context
	@param bucketName string - target bucket name
	@param objectKeys []string - target object names within the bucket
	@returns map of per-object-key deletion errors (empty if all succeeded)
	@returns error a function-level error, e.g. if the context is cancelled
	         before the bulk delete completes

On cancellation both returns may be non-nil: the map holds the per-object
failures reported before the delete stopped, and the error signals the
cancellation. When the error is non-nil the map is not exhaustive, so a
key's absence does not confirm it was deleted.
*/
func (s *s3ClientImpl) DeleteObjects(
	ctx context.Context, bucketName string, objectKeys []string,
) (map[string]error, error) {
	objectFeed := make(chan minio.ObjectInfo, len(objectKeys))
	for _, objectKey := range objectKeys {
		objectFeed <- minio.ObjectInfo{Key: objectKey}
	}
	close(objectFeed)

	resultFeed := s.s3.RemoveObjects(ctx, bucketName, objectFeed, minio.RemoveObjectsOptions{})

	deleteErrors := map[string]error{}

	// Wait for request complete
	for err := range resultFeed {
		deleteErrors[err.ObjectName] = NewObjectStoreError(
			fmt.Sprintf("failed to delete '%s/%s'", bucketName, err.ObjectName), err.Err, true,
		)
	}
	var exitError error
	if ctx.Err() != nil {
		exitError = NewRuntimeError("bulk delete context expired", ctx.Err(), true)
	}

	return deleteErrors, exitError
}

/*
GeneratePresignedGetURL generate presigned GET URL from S3 server for a particular
bucket and object key.

	@param ctx context.Context - execution context
	@param bucketName string - target bucket name
	@param objectKey string - target object key
	@param ttl time.Duration - TTL for the pre-signed URL (must be > 0 and <= 7 days)
	@returns presigned URL
*/
func (s *s3ClientImpl) GeneratePresignedGetURL(
	ctx context.Context, bucketName string, objectKey string, ttl time.Duration,
) (*url.URL, error) {
	if err := validatePresignTTL(ttl); err != nil {
		return nil, err
	}
	getURL, err := s.s3.PresignedGetObject(ctx, bucketName, objectKey, ttl, nil)
	if err != nil {
		return nil, NewObjectStoreError(
			fmt.Sprintf(
				"failed to generate presigned GET URL for s3://%s/%s", bucketName, objectKey,
			), err, true,
		)
	}
	return getURL, nil
}

/*
GeneratePresignedPutURL generate presigned PUT URL from S3 server for a particular
bucket and object key.

The returned URL binds the upload to an exact `Content-Length` and a base64-encoded
SHA-256 of the body (sent as the `x-amz-checksum-sha256` header). The HTTP client
using this URL must send both headers with these exact values, or the object store
will reject the request with a signature or checksum error.

	@param ctx context.Context - execution context
	@param bucketName string - target bucket name
	@param objectKey string - target object key
	@param objectSize int64 - exact size in bytes of the object the caller will upload
	@param sha256Sum string - base64-encoded SHA-256 of the object content
	@param ttl time.Duration - TTL for the pre-signed URL (must be > 0 and <= 7 days)
	@returns presigned URL
*/
func (s *s3ClientImpl) GeneratePresignedPutURL(
	ctx context.Context,
	bucketName string,
	objectKey string,
	objectSize int64,
	sha256Sum string,
	ttl time.Duration,
) (*url.URL, error) {
	if objectSize < 0 {
		return nil, NewBadInputError(
			fmt.Sprintf(
				"object size must be non-negative for presigned PUT URL of s3://%s/%s",
				bucketName, objectKey,
			),
			nil,
			true,
		)
	}
	if sha256Sum == "" {
		return nil, NewBadInputError(
			fmt.Sprintf(
				"sha256 checksum is required for presigned PUT URL of s3://%s/%s",
				bucketName, objectKey,
			),
			nil,
			true,
		)
	}
	// Sanity-check that the checksum is valid base64; a malformed value would
	// otherwise only fail later at upload time with a confusing server error.
	if _, err := base64.StdEncoding.DecodeString(sha256Sum); err != nil {
		return nil, NewBadInputError(
			fmt.Sprintf(
				"sha256 checksum must be valid base64 for presigned PUT URL of s3://%s/%s",
				bucketName, objectKey,
			),
			err,
			true,
		)
	}
	if err := validatePresignTTL(ttl); err != nil {
		return nil, err
	}

	signedHeaders := http.Header{}
	signedHeaders.Set("Content-Length", strconv.FormatInt(objectSize, 10))
	signedHeaders.Set("x-amz-checksum-sha256", sha256Sum)

	putURL, err := s.s3.PresignHeader(
		ctx, http.MethodPut, bucketName, objectKey, ttl, url.Values{}, signedHeaders,
	)
	if err != nil {
		return nil, NewObjectStoreError(
			fmt.Sprintf(
				"failed to generate presigned PUT URL for s3://%s/%s", bucketName, objectKey,
			), err, true,
		)
	}
	return putURL, nil
}
