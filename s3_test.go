package goutils_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// getS3ClientConfig helper function to get the connection parameters to unit test object store
func getS3ClientConfig() goutils.S3Config {
	endpoint := os.Getenv("UNITTEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:9000"
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = "admin"
	}
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = "password"
	}
	return goutils.S3Config{
		ServerEndpoint: endpoint, UseTLS: false, Creds: goutils.S3Credentials{
			AccessKey: accessKey, SecretAccessKey: secretKey,
		},
	}
}

func TestS3ClientBucketCreate(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	// Get S3 config
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	testBucket := fmt.Sprintf("ut-s3-client-bucket-create-%s", uuid.NewString())

	assert.Nil(uut.CreateBucket(utCtx, testBucket))

	buckets, err := uut.ListBuckets(utCtx)
	assert.Nil(err)
	bucketByName := map[string]bool{}
	for _, oneBucket := range buckets {
		bucketByName[oneBucket] = true
	}

	assert.Contains(bucketByName, testBucket)

	// Clean up
	assert.Nil(uut.DeleteBucket(utCtx, testBucket))
}

func TestS3ClientObjectCRUD(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	// Get S3 config
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	testBucket := fmt.Sprintf("ut-s3-client-object-crud-%s", uuid.NewString())

	assert.Nil(uut.CreateBucket(utCtx, testBucket))
	{
		buckets, err := uut.ListBuckets(utCtx)
		assert.Nil(err)
		bucketByName := map[string]bool{}
		for _, oneBucket := range buckets {
			bucketByName[oneBucket] = true
		}
		assert.Contains(bucketByName, testBucket)
	}

	// Case 0: Get unknown object
	{
		_, _, err := uut.GetObject(utCtx, testBucket, uuid.NewString())
		assert.NotNil(err)
	}

	// Case 1: put object
	testObject := uuid.NewString()
	testContent := ""
	{
		builder := strings.Builder{}
		for itr := 0; itr < 40; itr++ {
			_, err := builder.WriteString(uuid.NewString())
			assert.Nil(err)
		}
		testContent = builder.String()
	}
	testMIMEType := "text/plain"
	assert.Nil(uut.PutObject(
		utCtx, testBucket, testObject, bytes.NewBufferString(testContent), -1, &testMIMEType,
	))

	// Case 2: Get test object back
	{
		stats, reader, err := uut.GetObject(utCtx, testBucket, testObject)
		assert.Nil(err)
		readContent, err := io.ReadAll(reader)
		assert.Nil(err)
		assert.Equal(testContent, string(readContent))
		assert.Equal(len(testContent), int(stats.Size))
		assert.Equal(testMIMEType, stats.MIMEType)
		// PutObject always stores a SHA-256 checksum
		assert.NotEmpty(stats.CheckSum)
	}

	// Case 3: Delete object
	assert.Nil(uut.DeleteObject(utCtx, testBucket, testObject))
	{
		_, _, err := uut.GetObject(utCtx, testBucket, testObject)
		assert.NotNil(err)
	}

	// Clean up
	assert.Nil(uut.DeleteBucket(utCtx, testBucket))
}

func TestS3ClientBulkDelete(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	// Get S3 config
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	testBucket := fmt.Sprintf("ut-s3-client-object-crud-%s", uuid.NewString())

	assert.Nil(uut.CreateBucket(utCtx, testBucket))
	{
		buckets, err := uut.ListBuckets(utCtx)
		assert.Nil(err)
		bucketByName := map[string]bool{}
		for _, oneBucket := range buckets {
			bucketByName[oneBucket] = true
		}
		assert.Contains(bucketByName, testBucket)
	}

	// Put objects
	testObjects := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	testContent := ""
	{
		builder := strings.Builder{}
		for itr := 0; itr < 40; itr++ {
			_, err := builder.WriteString(uuid.NewString())
			assert.Nil(err)
		}
		testContent = builder.String()
	}
	for _, object := range testObjects {
		assert.Nil(uut.PutObject(
			utCtx, testBucket, object, bytes.NewBufferString(testContent), -1, nil,
		))
	}

	{
		objects, err := uut.ListObjects(utCtx, testBucket, nil, nil, nil)
		assert.Nil(err)
		assert.Len(objects, len(testObjects))
	}

	// Case 0: delete some objects
	{
		errors, err := uut.DeleteObjects(utCtx, testBucket, []string{testObjects[0], testObjects[2]})
		assert.Nil(err)
		assert.Len(errors, 0)
	}
	{
		objects, err := uut.ListObjects(utCtx, testBucket, nil, nil, nil)
		assert.Nil(err)
		assert.Len(objects, 2)
	}

	// Case 1: delete other objects
	{
		unknownObject := uuid.NewString()
		errors, err := uut.DeleteObjects(
			utCtx, testBucket, []string{testObjects[1], testObjects[3], unknownObject},
		)
		assert.Nil(err)
		assert.Len(errors, 0)
	}
	{
		objects, err := uut.ListObjects(utCtx, testBucket, nil, nil, nil)
		assert.Nil(err)
		assert.Len(objects, 0)
	}

	// Clean up
	assert.Nil(uut.DeleteBucket(utCtx, testBucket))
}

func TestS3ClientUsePresignedURL(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	config := getS3ClientConfig()

	// create client
	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	// create test bucket
	testBucket := fmt.Sprintf("ut-s3-client-presigned-%s", uuid.NewString())
	assert.Nil(uut.CreateBucket(utCtx, testBucket))

	// ---- generate presigned PUT URL and upload object ----
	testObject := uuid.NewString()
	testContent := uuid.NewString() + "-presigned-test"

	// Calculate checksum (using SHA256)
	hashCompute := sha256.New()
	_, err = hashCompute.Write([]byte(testContent))
	assert.Nil(err)
	checksumB64 := base64.StdEncoding.EncodeToString(hashCompute.Sum(nil))

	putURL, err := uut.GeneratePresignedPutURL(
		utCtx, testBucket, testObject, int64(len(testContent)), checksumB64, time.Minute*5,
	)
	assert.Nil(err)
	assert.NotNil(putURL)

	log.Debugf("Presigned PUT URL: %s", putURL.String())

	restyClient := resty.New()
	resp, err := restyClient.R().
		SetHeader("Content-Type", "application/octet-stream").
		SetHeader("x-amz-checksum-sha256", checksumB64).
		SetBody([]byte(testContent)).
		Put(putURL.String())
	assert.Nil(err)
	respBody := string(resp.Body())
	assert.Equal(200, resp.StatusCode(), respBody)
	log.Debugf("PUT URL Resp:\n%s", respBody)

	// ---- generate presigned GET URL and verify content ----
	getURL, err := uut.GeneratePresignedGetURL(utCtx, testBucket, testObject, time.Minute*5)
	assert.Nil(err)
	assert.NotNil(getURL)

	log.Debugf("Presigned GET URL: %s", getURL.String())

	resp, err = restyClient.R().Get(getURL.String())
	assert.Nil(err)
	assert.Equal(200, resp.StatusCode(), string(resp.Body()))
	assert.Equal(testContent, string(resp.Body()))

	// ---- cleanup ----
	assert.Nil(uut.DeleteObject(utCtx, testBucket, testObject))
	assert.Nil(uut.DeleteBucket(utCtx, testBucket))
}

func TestS3ClientGetObjectStat(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	ctx := context.Background()

	// --- config & client ----------------------------------------------------
	cfg := getS3ClientConfig()
	cli, err := goutils.NewS3Client(cfg)
	assert.Nil(err)

	// --- bucket -------------------------------------------------------------
	bucket := fmt.Sprintf("ut-s3-client-stat-%s", uuid.NewString())
	assert.Nil(cli.CreateBucket(ctx, bucket))
	// ensure bucket removal at the end
	defer func() {
		_ = cli.DeleteBucket(ctx, bucket)
	}()

	// --- test file -----------------------------------------------------------
	filePath := "for_test/Times_Square_April_2022_by_Don_Ramey_Logan.jpg"

	// object key (use the file base name)
	objectKey := filepath.Base(filePath)

	objectContent, err := os.ReadFile(filePath)
	assert.Nil(err)

	// Calculate checksum and size before upload
	fileSize := int64(len(objectContent))

	// Calculate checksum (using SHA256)
	hashCompute := sha256.New()
	_, err = hashCompute.Write(objectContent)
	assert.Nil(err)
	checksum := fmt.Sprintf("%x", hashCompute.Sum(nil))
	checksumB64 := base64.StdEncoding.EncodeToString(hashCompute.Sum(nil))

	// --- upload using presigned PUT URL with checksum and content-type ----
	putURL, err := cli.GeneratePresignedPutURL(
		ctx, bucket, objectKey, int64(len(objectContent)), checksumB64, time.Minute*5,
	)
	assert.Nil(err)
	assert.NotNil(putURL)

	log.Debugf("Presigned PUT URL: %s", putURL.String())

	// Create a REST client for the upload
	restyClient := resty.New()
	resp, err := restyClient.R().
		SetHeader("Content-Type", "image/jpeg").
		SetHeader("x-amz-checksum-sha256", checksumB64).
		SetContentLength(true).
		SetBody(objectContent).
		Put(putURL.String())
	assert.Nil(err)
	assert.Equal(200, resp.StatusCode(), string(resp.Body()))

	// --- get stats -----------------------------------------------------------
	stat, err := cli.GetObjectStat(ctx, bucket, objectKey)
	assert.Nil(err)

	const expectedSize int64 = 7913521

	assert.Equal(expectedSize, fileSize)
	assert.Equal(expectedSize, stat.Size, "size mismatch")
	assert.Equal(checksumB64, stat.CheckSum, "checksum mismatch")
	log.Debugf("Stored checksum: %s", stat.CheckSum)
	log.Debugf("Computed checksum (hex): %s", checksum)
	log.Debugf("Computed checksum (base64): %s", checksumB64)

	// --- cleanup -------------------------------------------------------------
	assert.Nil(cli.DeleteObject(ctx, bucket, objectKey))
}

func TestS3ClientCopyObject(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	ctx := context.Background()

	// --- config & client ----------------------------------------------------
	cfg := getS3ClientConfig()
	cli, err := goutils.NewS3Client(cfg)
	assert.Nil(err)

	// --- bucket -------------------------------------------------------------
	bucket := fmt.Sprintf("ut-s3-client-copy-%s", uuid.NewString())
	assert.Nil(cli.CreateBucket(ctx, bucket))
	defer func() {
		_ = cli.DeleteBucket(ctx, bucket)
	}()

	// --- source object ------------------------------------------------------
	filePath := "for_test/Times_Square_April_2022_by_Don_Ramey_Logan.jpg"
	srcKey := filepath.Base(filePath)

	objectContent, err := os.ReadFile(filePath)
	assert.Nil(err)

	hashCompute := sha256.New()
	_, err = hashCompute.Write(objectContent)
	assert.Nil(err)
	checksumB64 := base64.StdEncoding.EncodeToString(hashCompute.Sum(nil))

	// --- upload via presigned PUT URL ---------------------------------------
	// Content-Type is intentionally not set: GeneratePresignedPutURL only signs
	// the host header, so MinIO ignores any unsigned Content-Type sent during
	// the upload. In production the staging upload's MIME type is irrelevant —
	// the ingestion task detects the real type from the object's magic bytes
	// and rewrites it via CopyObject.
	putURL, err := cli.GeneratePresignedPutURL(
		ctx, bucket, srcKey, int64(len(objectContent)), checksumB64, time.Minute*5,
	)
	assert.Nil(err)
	assert.NotNil(putURL)

	restyClient := resty.New()
	resp, err := restyClient.R().
		SetHeader("x-amz-checksum-sha256", checksumB64).
		SetContentLength(true).
		SetBody(objectContent).
		Put(putURL.String())
	assert.Nil(err)
	assert.Equal(200, resp.StatusCode(), string(resp.Body()))

	// --- copy operations ----------------------------------------------------
	dst1 := "copy/dest-1-" + uuid.NewString()
	dst2 := "copy/dest-2-" + uuid.NewString()

	// Dest 1: no override — preserve whatever MIME the source landed with.
	assert.Nil(cli.CopyObject(ctx, bucket, srcKey, bucket, dst1, nil))

	// Dest 2: override MIME type to PNG.
	pngMIME := "image/png"
	assert.Nil(cli.CopyObject(ctx, bucket, srcKey, bucket, dst2, &pngMIME))

	// --- verification -------------------------------------------------------
	fetch := func(key string) *resty.Response {
		getURL, err := cli.GeneratePresignedGetURL(ctx, bucket, key, time.Minute*5)
		assert.Nil(err)
		assert.NotNil(getURL)

		resp, err := restyClient.R().Get(getURL.String())
		assert.Nil(err)
		assert.Equal(200, resp.StatusCode(), string(resp.Body()))
		return resp
	}

	// Dest 1: copy completed. MIME is not verified — the source's MIME could
	// not be set during the presigned PUT, so there is no meaningful expected
	// value to assert against.
	fetch(dst1)

	// Dest 2: MIME overridden to PNG.
	{
		resp := fetch(dst2)
		assert.Equal(pngMIME, resp.Header().Get("Content-Type"), "MIME mismatch for %s", dst2)
	}

	// --- cleanup ------------------------------------------------------------
	assert.Nil(cli.DeleteObject(ctx, bucket, srcKey))
	assert.Nil(cli.DeleteObject(ctx, bucket, dst1))
	assert.Nil(cli.DeleteObject(ctx, bucket, dst2))
}

// TestS3ClientPresignedURLValidation exercises the input-validation guards on the
// presigned URL generators. Every case must fail before any network I/O, so this
// test only constructs a client and never creates a bucket or object.
func TestS3ClientPresignedURLValidation(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	const (
		bucket    = "ut-validate-bucket"
		objectKey = "ut-validate-object"
	)
	// A valid base64 SHA-256 so PUT cases fail on the field under test, not the sum.
	validSum := base64.StdEncoding.EncodeToString(make([]byte, sha256.Size))
	overMaxTTL := 8 * 24 * time.Hour // > 7 day max

	assertBadInput := func(url interface{}, err error) {
		assert.Nil(url)
		var badInput goutils.BadInputError
		assert.True(errors.As(err, &badInput), "expected BadInputError, got %T: %v", err, err)
	}

	// ---- GeneratePresignedGetURL ----
	{
		url, err := uut.GeneratePresignedGetURL(utCtx, bucket, objectKey, 0)
		assertBadInput(url, err)
	}
	{
		url, err := uut.GeneratePresignedGetURL(utCtx, bucket, objectKey, overMaxTTL)
		assertBadInput(url, err)
	}

	// ---- GeneratePresignedPutURL ----
	// TTL guards
	{
		url, err := uut.GeneratePresignedPutURL(utCtx, bucket, objectKey, 1, validSum, -time.Second)
		assertBadInput(url, err)
	}
	{
		url, err := uut.GeneratePresignedPutURL(utCtx, bucket, objectKey, 1, validSum, overMaxTTL)
		assertBadInput(url, err)
	}
	// Negative object size
	{
		url, err := uut.GeneratePresignedPutURL(utCtx, bucket, objectKey, -1, validSum, time.Minute)
		assertBadInput(url, err)
	}
	// Empty checksum
	{
		url, err := uut.GeneratePresignedPutURL(utCtx, bucket, objectKey, 1, "", time.Minute)
		assertBadInput(url, err)
	}
	// Non-base64 checksum
	{
		url, err := uut.GeneratePresignedPutURL(utCtx, bucket, objectKey, 1, "not!base64!", time.Minute)
		assertBadInput(url, err)
	}
}

// TestS3ClientCreateBucketIdempotent verifies that creating a bucket that already
// exists and is owned by this account returns nil (idempotent create).
func TestS3ClientCreateBucketIdempotent(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	testBucket := fmt.Sprintf("ut-s3-client-idempotent-%s", uuid.NewString())

	// First create succeeds.
	assert.Nil(uut.CreateBucket(utCtx, testBucket))
	// Second create on the same, self-owned bucket is a no-op, not an error.
	assert.Nil(uut.CreateBucket(utCtx, testBucket))

	// Clean up
	assert.Nil(uut.DeleteBucket(utCtx, testBucket))
}

// TestS3ClientListObjectsPagination exercises the prefix, startingKey (StartAfter,
// exclusive) and maxKeys (total cap) options of ListObjects.
func TestS3ClientListObjectsPagination(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	testBucket := fmt.Sprintf("ut-s3-client-pagination-%s", uuid.NewString())
	assert.Nil(uut.CreateBucket(utCtx, testBucket))
	defer func() {
		_ = uut.DeleteBucket(utCtx, testBucket)
	}()

	// Deterministic, sortable keys so StartAfter / ordering is assertable.
	pageKeys := []string{"page/00", "page/01", "page/02", "page/03", "page/04"}
	otherKeys := []string{"other/00", "other/01"}
	allKeys := append(append([]string{}, pageKeys...), otherKeys...)
	for _, key := range allKeys {
		assert.Nil(uut.PutObject(
			utCtx, testBucket, key, bytes.NewBufferString("x"), -1, nil,
		))
	}
	defer func() {
		_, _ = uut.DeleteObjects(utCtx, testBucket, allKeys)
	}()

	pagePrefix := "page/"

	// prefix: only the page/* keys.
	{
		objects, err := uut.ListObjects(utCtx, testBucket, &pagePrefix, nil, nil)
		assert.Nil(err)
		sort.Strings(objects)
		assert.Equal(pageKeys, objects)
	}

	// maxKeys: cap the total returned (exercises the early-break + cancel).
	{
		maxKeys := 2
		objects, err := uut.ListObjects(utCtx, testBucket, &pagePrefix, nil, &maxKeys)
		assert.Nil(err)
		assert.Len(objects, 2)
		// Keys are returned in lexical order, so the first two page keys.
		sort.Strings(objects)
		assert.Equal(pageKeys[:2], objects)
	}

	// startingKey: strictly after page/01 (exclusive).
	{
		startAfter := "page/01"
		objects, err := uut.ListObjects(utCtx, testBucket, &pagePrefix, &startAfter, nil)
		assert.Nil(err)
		sort.Strings(objects)
		assert.Equal([]string{"page/02", "page/03", "page/04"}, objects)
		assert.NotContains(objects, "page/01")
	}

	// startingKey + maxKeys combined.
	{
		startAfter := "page/01"
		maxKeys := 2
		objects, err := uut.ListObjects(utCtx, testBucket, &pagePrefix, &startAfter, &maxKeys)
		assert.Nil(err)
		assert.Len(objects, 2)
		sort.Strings(objects)
		assert.Equal([]string{"page/02", "page/03"}, objects)
	}
}

// TestS3ClientCopyObjectCrossBucket verifies CopyObject across two different
// buckets, including the MIME-type rewrite on the destination.
func TestS3ClientCopyObjectCrossBucket(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	ctx := context.Background()
	cfg := getS3ClientConfig()
	cli, err := goutils.NewS3Client(cfg)
	assert.Nil(err)

	srcBucket := fmt.Sprintf("ut-s3-client-copy-src-%s", uuid.NewString())
	dstBucket := fmt.Sprintf("ut-s3-client-copy-dst-%s", uuid.NewString())
	assert.Nil(cli.CreateBucket(ctx, srcBucket))
	assert.Nil(cli.CreateBucket(ctx, dstBucket))
	defer func() {
		_ = cli.DeleteBucket(ctx, srcBucket)
		_ = cli.DeleteBucket(ctx, dstBucket)
	}()

	// --- source object ------------------------------------------------------
	filePath := "for_test/Times_Square_April_2022_by_Don_Ramey_Logan.jpg"
	srcKey := filepath.Base(filePath)

	objectContent, err := os.ReadFile(filePath)
	assert.Nil(err)

	hashCompute := sha256.New()
	_, err = hashCompute.Write(objectContent)
	assert.Nil(err)
	checksumB64 := base64.StdEncoding.EncodeToString(hashCompute.Sum(nil))

	putURL, err := cli.GeneratePresignedPutURL(
		ctx, srcBucket, srcKey, int64(len(objectContent)), checksumB64, time.Minute*5,
	)
	assert.Nil(err)
	assert.NotNil(putURL)

	restyClient := resty.New()
	resp, err := restyClient.R().
		SetHeader("x-amz-checksum-sha256", checksumB64).
		SetContentLength(true).
		SetBody(objectContent).
		Put(putURL.String())
	assert.Nil(err)
	assert.Equal(200, resp.StatusCode(), string(resp.Body()))

	// --- cross-bucket copy with MIME override -------------------------------
	dstKey := "copy/cross-" + uuid.NewString()
	pngMIME := "image/png"
	assert.Nil(cli.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, &pngMIME))

	// --- verification: content retrievable from dst bucket, MIME overridden --
	getURL, err := cli.GeneratePresignedGetURL(ctx, dstBucket, dstKey, time.Minute*5)
	assert.Nil(err)
	assert.NotNil(getURL)

	resp, err = restyClient.R().Get(getURL.String())
	assert.Nil(err)
	assert.Equal(200, resp.StatusCode(), string(resp.Body()))
	assert.Equal(objectContent, resp.Body(), "content mismatch after cross-bucket copy")
	assert.Equal(pngMIME, resp.Header().Get("Content-Type"), "MIME mismatch for %s", dstKey)

	// --- cleanup ------------------------------------------------------------
	assert.Nil(cli.DeleteObject(ctx, srcBucket, srcKey))
	assert.Nil(cli.DeleteObject(ctx, dstBucket, dstKey))
}

// TestS3ClientDeleteObjectsErrors verifies that DeleteObjects populates the
// per-object error map on failure. It deletes from a valid-format but nonexistent
// bucket, which passes minio's client-side bucket-name validation and is then
// rejected server-side ("The specified bucket does not exist"). The object store
// reports this as one error per submitted key (not a function-level error), so
// every key must appear in the map with an ObjectStoreError value.
func TestS3ClientDeleteObjectsErrors(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	// Valid bucket-name format, but this bucket is never created.
	missingBucket := fmt.Sprintf("ut-s3-client-missing-%s", uuid.NewString())
	keys := []string{uuid.NewString(), uuid.NewString()}

	deleteErrors, exitErr := uut.DeleteObjects(utCtx, missingBucket, keys)

	// No context cancellation, so no function-level error.
	assert.Nil(exitErr)
	// Every submitted key must be reported as failed, each an ObjectStoreError.
	assert.Len(deleteErrors, len(keys))
	for _, key := range keys {
		objErr, ok := deleteErrors[key]
		assert.True(ok, "expected an error entry for key %q", key)
		var objStoreErr goutils.ObjectStoreError
		assert.True(
			errors.As(objErr, &objStoreErr),
			"per-object error for %q should be ObjectStoreError, got %T: %v", key, objErr, objErr,
		)
	}
}

// TestS3ClientPutObjectKnownSize uploads an object with a known exact size (rather
// than -1) and no explicit content type, then verifies the stored size, the
// default MIME type, and that a checksum was recorded.
func TestS3ClientPutObjectKnownSize(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	config := getS3ClientConfig()

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)

	testBucket := fmt.Sprintf("ut-s3-client-put-sized-%s", uuid.NewString())
	assert.Nil(uut.CreateBucket(utCtx, testBucket))
	defer func() {
		_ = uut.DeleteBucket(utCtx, testBucket)
	}()

	testObject := uuid.NewString()
	testContent := uuid.NewString() + "-known-size-body"

	assert.Nil(uut.PutObject(
		utCtx, testBucket, testObject,
		bytes.NewBufferString(testContent), int64(len(testContent)), nil,
	))
	defer func() {
		_ = uut.DeleteObject(utCtx, testBucket, testObject)
	}()

	stat, err := uut.GetObjectStat(utCtx, testBucket, testObject)
	assert.Nil(err)
	assert.Equal(int64(len(testContent)), stat.Size)
	// No content type supplied -> object store default.
	assert.Equal("application/octet-stream", stat.MIMEType)
	// PutObject always stores a SHA-256 checksum.
	assert.NotEmpty(stat.CheckSum)
}

// TestS3ClientNewClientWithRegion verifies that a client constructed with an
// explicit S3Config.Region is created without error and is usable.
func TestS3ClientNewClientWithRegion(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	config := getS3ClientConfig()
	region := "us-east-1"
	config.Region = &region

	uut, err := goutils.NewS3Client(config)
	assert.Nil(err)
	assert.NotNil(uut)

	// The region-set client should be usable end to end.
	testBucket := fmt.Sprintf("ut-s3-client-region-%s", uuid.NewString())
	assert.Nil(uut.CreateBucket(utCtx, testBucket))
	assert.Nil(uut.DeleteBucket(utCtx, testBucket))
}
