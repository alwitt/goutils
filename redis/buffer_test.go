package redis_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"io"
	"testing"
	"time"

	"github.com/alwitt/goutils/redis"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRedisRingBufferBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 128)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Write into buffer
	testData01 := uuid.NewString()
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, []byte(testData01))
		assert.Nil(err)
		assert.Equal(len(testData01), int(written))

		lclCtxCancel()
	}

	// Read out of buffer
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, len(testData01)*2)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(0, int(actualOffset))
		assert.Equal(len(testData01), read)
		assert.Equal(len(testData01), int(total))
		assert.Equal([]byte(testData01), recvBuf[:read])

		lclCtxCancel()
	}
}

func TestRedisRingBufferOverflow(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 128)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Write 100 bytes of 'A' into the buffer
	testDataA := bytes.Repeat([]byte("A"), 100)
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testDataA)
		assert.Nil(err)
		assert.Equal(len(testDataA), int(written))

		lclCtxCancel()
	}

	// Write 100 bytes of 'B' into the buffer. With the 100 bytes of 'A' already present,
	// the total writes (200) exceed the 128 byte capacity, so the write wraps around.
	testDataB := bytes.Repeat([]byte("B"), 100)
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testDataB)
		assert.Nil(err)
		assert.Equal(len(testDataB), int(written))

		lclCtxCancel()
	}

	// Read 100 bytes starting from offset 0. As only the most recent 128 bytes survive,
	// the oldest readable byte is at offset 72 (200 - 128). The read therefore starts at
	// offset 72 and returns the last 28 bytes of 'A' followed by the 72 bytes of 'B'.
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, 100)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(72, int(actualOffset))
		assert.Equal(100, read)
		assert.Equal(200, int(total))

		expected := append(bytes.Repeat([]byte("A"), 28), bytes.Repeat([]byte("B"), 72)...)
		assert.Equal(expected, recvBuf[:read])

		lclCtxCancel()
	}
}

func TestRedisRingBufferBytesRepresentation(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 128)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Generate 100 bytes of random (non-ASCII) data
	testData := make([]byte, 100)
	_, err = rand.Read(testData)
	assert.Nil(err)

	// Write the random bytes into the buffer
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testData)
		assert.Nil(err)
		assert.Equal(len(testData), int(written))

		lclCtxCancel()
	}

	// Read the bytes back and verify they round-trip through REDIS unchanged
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, len(testData))

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(0, int(actualOffset))
		assert.Equal(len(testData), read)
		assert.Equal(len(testData), int(total))
		assert.Equal(testData, recvBuf[:read])

		lclCtxCancel()
	}
}

func TestRedisRingBufferEmptyRead(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 128)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Read from an empty buffer. Nothing has been written, so no data is returned.
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, 50)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(0, int(actualOffset))
		assert.Equal(0, read)
		assert.Equal(0, int(total))

		lclCtxCancel()
	}

	// Write 100 bytes into the buffer
	testData := bytes.Repeat([]byte("A"), 100)
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testData)
		assert.Nil(err)
		assert.Equal(len(testData), int(written))

		lclCtxCancel()
	}

	// Read starting at offset 120, which is beyond all 100 bytes written. No data is
	// returned, but the total written is still reported as 100.
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, 50)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 120)
		assert.Nil(err)

		assert.Equal(100, int(actualOffset))
		assert.Equal(0, read)
		assert.Equal(100, int(total))

		lclCtxCancel()
	}
}

func TestRedisRingBufferPartialRead(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	// makeSeq builds a byte slice of length n where each byte is its index, so the read
	// back data can be matched against an exact position in the written stream.
	makeSeq := func(n int) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = byte(i)
		}
		return out
	}

	// Case 1: over-read before the buffer wraps. Only the 20 bytes actually written
	// are returned, even though 30 were requested.
	{
		testBuffer := uuid.NewString()

		uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
		assert.Nil(err)
		defer func() {
			assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
		}()

		testData := makeSeq(20)
		{
			lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

			written, err := uut.Write(lclCtx, testData)
			assert.Nil(err)
			assert.Equal(len(testData), int(written))

			lclCtxCancel()
		}

		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)
		defer lclCtxCancel()

		recvBuf := make([]byte, 30)
		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(0, int(actualOffset))
		assert.Equal(20, read)
		assert.Equal(20, int(total))
		assert.Equal(testData, recvBuf[:read])
	}

	// Case 2: read greater than capacity. With 60 bytes written into a 50 byte buffer,
	// only the most recent 50 bytes survive and can be read back.
	{
		testBuffer := uuid.NewString()

		uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
		assert.Nil(err)
		defer func() {
			assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
		}()

		testData := makeSeq(60)
		{
			lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

			written, err := uut.Write(lclCtx, testData)
			assert.Nil(err)
			assert.Equal(len(testData), int(written))

			lclCtxCancel()
		}

		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)
		defer lclCtxCancel()

		recvBuf := make([]byte, 60)
		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(10, int(actualOffset))
		assert.Equal(50, read)
		assert.Equal(60, int(total))
		assert.Equal(testData[10:], recvBuf[:read])
	}

	// Case 3: offset and capacity limit the read. With 50 bytes written, reading from
	// offset 20 returns only the last 30 bytes written.
	{
		testBuffer := uuid.NewString()

		uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
		assert.Nil(err)
		defer func() {
			assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
		}()

		testData := makeSeq(50)
		{
			lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

			written, err := uut.Write(lclCtx, testData)
			assert.Nil(err)
			assert.Equal(len(testData), int(written))

			lclCtxCancel()
		}

		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)
		defer lclCtxCancel()

		recvBuf := make([]byte, 60)
		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 20)
		assert.Nil(err)

		assert.Equal(20, int(actualOffset))
		assert.Equal(30, read)
		assert.Equal(50, int(total))
		assert.Equal(testData[20:], recvBuf[:read])
	}
}

func TestRedisRingBufferCapacityLimits(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	const maxCapacity = 512 * 1024 * 1024

	// Capacities outside the [1, 512MiB] range are rejected
	{
		_, err := client.GetRingBuffer(utCtx, uuid.NewString(), 0)
		assert.Error(err)

		_, err = client.GetRingBuffer(utCtx, uuid.NewString(), -1)
		assert.Error(err)

		_, err = client.GetRingBuffer(utCtx, uuid.NewString(), maxCapacity+1)
		assert.Error(err)
	}

	// Capacities at the boundaries of the [1, 512MiB] range are accepted
	{
		uut, err := client.GetRingBuffer(utCtx, uuid.NewString(), 1)
		assert.Nil(err)
		assert.NotNil(uut)

		uut, err = client.GetRingBuffer(utCtx, uuid.NewString(), maxCapacity)
		assert.Nil(err)
		assert.NotNil(uut)
	}
}

func TestRedisRingBufferWriteCountTrack(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 100)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// An empty write stores nothing and is reported as zero bytes written
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, []byte{})
		assert.Nil(err)
		assert.Equal(0, int(written))

		lclCtxCancel()
	}

	// Total reports zero before anything is written
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		total, err := uut.Total(lclCtx)
		assert.Nil(err)
		assert.Equal(0, int(total))

		lclCtxCancel()
	}

	// Write 130 bytes into the 100 byte buffer
	testData := make([]byte, 130)
	for i := range testData {
		testData[i] = byte(i)
	}
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testData)
		assert.Nil(err)
		assert.Equal(len(testData), int(written))

		lclCtxCancel()
	}

	// Total tracks the full count written, even though only 100 bytes can be retained
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		total, err := uut.Total(lclCtx)
		assert.Nil(err)
		assert.Equal(130, int(total))

		lclCtxCancel()
	}

	// Read 130 bytes starting at 0. Only the most recent 100 bytes survive, so the read
	// starts at offset 30 (130 - 100) and returns the last 100 bytes written.
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, 130)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(30, int(actualOffset))
		assert.Equal(100, read)
		assert.Equal(130, int(total))
		assert.Equal(testData[30:], recvBuf[:read])

		lclCtxCancel()
	}
}

func TestRedisRingBufferBufferReuse(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Write 50 bytes through the first handle
	testData := make([]byte, 50)
	for i := range testData {
		testData[i] = byte(i)
	}
	{
		uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
		assert.Nil(err)

		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testData)
		assert.Nil(err)
		assert.Equal(len(testData), int(written))

		lclCtxCancel()
	}

	// A second handle pointing at the same buffer name sees the previously written data,
	// since the buffer state lives in REDIS
	{
		uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
		assert.Nil(err)

		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, 100)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 0)
		assert.Nil(err)

		assert.Equal(0, int(actualOffset))
		assert.Equal(50, read)
		assert.Equal(50, int(total))
		assert.Equal(testData, recvBuf[:read])

		lclCtxCancel()
	}

	// Delete the buffer, then recreate a handle under the same name. The buffer is empty
	// again, as the underlying REDIS keys were removed.
	{
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))

		uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
		assert.Nil(err)

		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		total, err := uut.Total(lclCtx)
		assert.Nil(err)
		assert.Equal(0, int(total))

		lclCtxCancel()
	}
}

func TestRedisRingBufferLongWrites(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 1000)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Prepare 32768 bytes of random test data
	testData := make([]byte, 32768)
	_, err = rand.Read(testData)
	assert.Nil(err)

	// Track the data hash across the writing and reading phases independently. As the
	// reader keeps pace with the writer (512 byte reads against a 1000 byte buffer), every
	// byte written should be read back exactly once and in order.
	writeHash := sha1.New()
	readHash := sha1.New()

	const chunkSize = 512
	var readOffset int64
	for start := 0; start < len(testData); start += chunkSize {
		chunk := testData[start : start+chunkSize]

		// Writing phase: update the write hash and push the chunk into the buffer
		{
			lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

			written, err := uut.Write(lclCtx, chunk)
			assert.Nil(err)
			assert.Equal(len(chunk), int(written))

			_, _ = writeHash.Write(chunk)

			lclCtxCancel()
		}

		// Reading phase: pull a chunk back from the tracked offset and update the read hash
		{
			lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

			recvBuf := make([]byte, chunkSize)

			actualOffset, read, _, err := uut.ReadAt(lclCtx, recvBuf, readOffset)
			assert.Nil(err)
			// the reader never falls behind the live window, so the offset is honored exactly
			assert.Equal(readOffset, actualOffset)

			_, _ = readHash.Write(recvBuf[:read])
			readOffset = actualOffset + int64(read)

			lclCtxCancel()
		}
	}

	// Both phases observed the same bytes in the same order
	assert.Equal(writeHash.Sum(nil), readHash.Sum(nil))
}

func TestRedisRingBufferAsStdReadWrite(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 1000)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Wrap the buffer behind the standard `io.ReadWriteCloser` interface
	rwc := uut.AsReadWriteCloser(utCtx, 0, time.Millisecond)

	// Prepare 32768 bytes of random test data
	testData := make([]byte, 32768)
	_, err = rand.Read(testData)
	assert.Nil(err)

	// Track the data hash across the writing and reading phases independently. As the
	// reader keeps pace with the writer (512 byte reads against a 1000 byte buffer), every
	// byte written should be read back exactly once and in order.
	writeHash := sha1.New()
	readHash := sha1.New()

	const chunkSize = 512
	for start := 0; start < len(testData); start += chunkSize {
		chunk := testData[start : start+chunkSize]

		// Writing phase: update the write hash and push the chunk through `io.Writer`
		{
			written, err := rwc.Write(chunk)
			assert.Nil(err)
			assert.Equal(len(chunk), written)

			_, _ = writeHash.Write(chunk)
		}

		// Reading phase: pull the chunk back through `io.Reader`. The wrapper blocks until
		// data is available, so a single read returns the freshly written chunk. `io.ReadFull`
		// guards against a short read returning fewer than `chunkSize` bytes.
		{
			recvBuf := make([]byte, chunkSize)

			read, err := io.ReadFull(rwc, recvBuf)
			assert.Nil(err)
			assert.Equal(chunkSize, read)

			_, _ = readHash.Write(recvBuf[:read])
		}
	}

	// Both phases observed the same bytes in the same order
	assert.Equal(writeHash.Sum(nil), readHash.Sum(nil))

	// Closing the wrapper cancels its working context: subsequent reads report a clean EOF
	// and writes report a closed pipe.
	assert.Nil(rwc.Close())
	{
		read, err := rwc.Read(make([]byte, chunkSize))
		assert.Equal(0, read)
		assert.ErrorIs(err, io.EOF)

		written, err := rwc.Write([]byte("data after close"))
		assert.Equal(0, written)
		assert.ErrorIs(err, io.ErrClosedPipe)
	}
}

func TestRedisRingBufferEmptyReadBuffer(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Write 80 bytes into the 50 byte buffer
	testData := make([]byte, 80)
	for i := range testData {
		testData[i] = byte(i)
	}
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testData)
		assert.Nil(err)
		assert.Equal(len(testData), int(written))

		lclCtxCancel()
	}

	// Read with a zero-length receive buffer starting at offset 10. No data can be copied,
	// but the offset is still clamped into the live window (oldest = 80 - 50 = 30) and the
	// total written is reported correctly.
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, 0)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, 10)
		assert.Nil(err)

		assert.Equal(30, int(actualOffset))
		assert.Equal(0, read)
		assert.Equal(80, int(total))

		lclCtxCancel()
	}
}

func TestRedisRingBufferNegativeOffsetRead(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testBuffer := uuid.NewString()

	uut, err := client.GetRingBuffer(utCtx, testBuffer, 50)
	assert.Nil(err)
	defer func() {
		assert.Nil(client.DeleteRingBuffer(utCtx, testBuffer))
	}()

	// Write 50 bytes into the buffer
	testData := make([]byte, 50)
	for i := range testData {
		testData[i] = byte(i)
	}
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		written, err := uut.Write(lclCtx, testData)
		assert.Nil(err)
		assert.Equal(len(testData), int(written))

		lclCtxCancel()
	}

	// Read with a negative offset. The offset is clamped up to the oldest live byte (0),
	// so the read behaves identically to reading from offset 0.
	{
		lclCtx, lclCtxCancel := context.WithTimeout(utCtx, time.Millisecond*30)

		recvBuf := make([]byte, 80)

		actualOffset, read, total, err := uut.ReadAt(lclCtx, recvBuf, -10)
		assert.Nil(err)

		assert.Equal(0, int(actualOffset))
		assert.Equal(50, read)
		assert.Equal(50, int(total))
		assert.Equal(testData, recvBuf[:read])

		lclCtxCancel()
	}
}
