// Package redis - REDIS client package
package redis

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/alwitt/goutils"
	"github.com/redis/go-redis/v9"
)

var (
	// writeScript the REDIS script for writing into a ring buffer
	writeScript = redis.NewScript(
		`-- KEYS[1]=buf, KEYS[2]=meta ; ARGV[1]=cap, ARGV[2]=chunk
-- Returns: bytes written
local cap   = tonumber(ARGV[1])
local chunk = ARGV[2]
local dlen  = #chunk
if dlen == 0 then return 0 end

-- a single chunk bigger than the ring: only its last "cap" bytes can survive
local L = dlen
if L > cap then
  chunk = string.sub(chunk, dlen - cap + 1)
  L = cap
end

local total = tonumber(redis.call('GET', KEYS[2]) or '0')
-- but when a chunk is larger than the entire ring, only its last cap bytes can ever be read,
-- so we drop the front and place the survivors where they actually belong in the absolute
-- stream. This keeps reader and writer agreed on byte indices.
local pos   = (total + dlen - L) % cap

if pos + L <= cap then
  redis.call('SETRANGE', KEYS[1], pos, chunk)
else                                        -- straddles the end → two writes
  local first = cap - pos
  redis.call('SETRANGE', KEYS[1], pos, string.sub(chunk, 1, first))
  redis.call('SETRANGE', KEYS[1], 0,    string.sub(chunk, first + 1))
end

redis.call('SET', KEYS[2], total + dlen)
return dlen`,
	)

	// readScript the REDIS script for reading from a ring buffer at an offset
	readScript = redis.NewScript(
		`-- KEYS[1]=buf, KEYS[2]=meta ; ARGV[1]=cap, ARGV[2]=offset, ARGV[3]=maxlen
-- Returns: {start, read count, data}
local cap    = tonumber(ARGV[1])
local offset = tonumber(ARGV[2])
local maxlen = tonumber(ARGV[3])

local total  = tonumber(redis.call('GET', KEYS[2]) or '0')
local oldest = math.max(0, total - cap)

local start = math.max(oldest, math.min(offset, total))   -- clamp into the live window
local n     = math.min(maxlen, total - start)
if n <= 0 then return {start, total, ''} end              -- caught up / nothing to read

local pos = start % cap                                    -- physical start of the read
local data
if pos + n <= cap then
  data = redis.call('GETRANGE', KEYS[1], pos, pos + n - 1)
else                                                       -- wraps the physical end
  local first = cap - pos
  data = redis.call('GETRANGE', KEYS[1], pos, cap - 1)
      .. redis.call('GETRANGE', KEYS[1], 0, n - first - 1)
end
return {start, total, data}`,
	)

	// readNewestScript the REDIS script for reading the newest N bytes from a ring buffer
	readNewestScript = redis.NewScript(
		`-- KEYS[1]=buf, KEYS[2]=meta ; ARGV[1]=cap, ARGV[2]=maxlen
-- Returns: {start, read count, data}
local cap    = tonumber(ARGV[1])
local maxlen = tonumber(ARGV[2])

local total  = tonumber(redis.call('GET', KEYS[2]) or '0')
local oldest = math.max(0, total - cap)

local start = math.max(oldest, total - maxlen)            -- newest maxlen bytes, clamped to window
local n     = total - start
if n <= 0 then return {start, total, ''} end              -- nothing written yet

local pos = start % cap                                    -- physical start of the read
local data
if pos + n <= cap then
  data = redis.call('GETRANGE', KEYS[1], pos, pos + n - 1)
else                                                       -- wraps the physical end
  local first = cap - pos
  data = redis.call('GETRANGE', KEYS[1], pos, cap - 1)
      .. redis.call('GETRANGE', KEYS[1], 0, n - first - 1)
end
return {start, total, data}`,
	)
)

// RingBuffer implement a ring buffer based on REDIS `SETRANGE` and `GETRANGE`
type RingBuffer interface {
	/*
		Write a byte slice into the buffer.

		When the data currently in the buffer plus the new slice exceed the buffer capacity,
		the write will wrap around.

		If the byte slice is longer than the capacity of the ring buffer, N, then only the last N
		bytes of the slice is stored.

			@param ctx context.Context - execution context
			@param data []byte - data to write into the buffer
	*/
	Write(ctx context.Context, data []byte) (int64, error)

	/*
		ReadAt read data from the buffer starting at an offset.

		If the offset is older than the currently oldest byte in the buffer,  the offset
		is moved to the oldest byte currently in the buffer.

		If the offset is greater than all data written, nothing is returned.

			@param ctx context.Context - execution context
			@param buf []byte - the receive buffer
			@returns the actual offset
			@returns the number of bytes read
			@returns the total number of bytes written to the buffer
			@returns `REDISError` in case of failure
	*/
	ReadAt(ctx context.Context, buf []byte, offset int64) (int64, int, int64, error)

	/*
		ReadNewest read the newest bytes from the buffer, filling at most `len(buf)` bytes.

		If fewer than `len(buf)` bytes have been written, all available bytes are returned. If
		more than the buffer capacity is requested, only the bytes currently in the buffer are
		returned.

			@param ctx context.Context - execution context
			@param buf []byte - the receive buffer
			@returns the actual offset of the first byte read
			@returns the number of bytes read
			@returns the total number of bytes written to the buffer
			@returns `REDISError` in case of failure
	*/
	ReadNewest(ctx context.Context, buf []byte) (int64, int, int64, error)

	/*
		Total fetch the total bytes written to the buffer

			@param ctx context.Context - execution context
			@returns the total number of bytes written to the buffer
			@returns `REDISError` in case of failure
	*/
	Total(ctx context.Context) (int64, error)

	/*
		AsReadWriteCloser create a wrapper around this buffer so it presents an `io.ReadWriteCloser`
		interface, allowing this buffer to be used other standard IO helper functions.

			@param parentCtx context.Context - the parent execution context for this buffer
			    which allows for clean execution control of the buffer while maintaining
			    standard interface.
			@param initReadOffset int64 - define the starting read index offset on the buffer
			@param dataCheckInt time.Duration - as the buffer `ReadAt` API is non-blocking,
			    this set the interval of the internal polling loop for available data. Minimum
			    value is 1ms.
			@returns interface complaint with `io.ReadWriteCloser`
	*/
	AsReadWriteCloser(
		parentCtx context.Context, initReadOffset int64, dataCheckInt time.Duration,
	) io.ReadWriteCloser
}

type ringBuffer struct {
	goutils.Component

	// bufferNameKey name of the REDIS key holding the "string" buffer
	bufferNameKey string
	// bufferWrittenKey name of the REDIS key holding the total writes to the buffer
	bufferWrittenKey string

	// capacity of the buffer. As a ring buffer, write will wrap once the capacity is reached
	capacity int64

	core *redis.Client
}

/*
Write a byte slice into the buffer.

When the data currently in the buffer plus the new slice exceed the buffer capacity,
the write will wrap around.

If the byte slice is longer than the capacity of the ring buffer, N, then only the last N
bytes of the slice is stored.

	@param ctx context.Context - execution context
	@param data []byte - data to write into the buffer
	@returns the number of bytes written
	@returns `REDISError` in case of failure
*/
func (b *ringBuffer) Write(ctx context.Context, data []byte) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	written, err := writeScript.Run(
		ctx, b.core, []string{b.bufferNameKey, b.bufferWrittenKey}, b.capacity, data,
	).Int64()
	if err != nil {
		return 0, goutils.NewRedisError("failed to write to buffer "+b.bufferNameKey, err, true)
	}

	return written, nil
}

/*
ReadAt read data from the buffer starting at an offset.

If the offset is older than the currently oldest byte in the buffer,  the offset
is moved to the oldest byte currently in the buffer.

If the offset is greater than all data written, nothing is returned.

	@param ctx context.Context - execution context
	@param buf []byte - the receive buffer
	@returns the actual offset
	@returns the number of bytes read
	@returns the total number of bytes written to the buffer
	@returns `REDISError` in case of failure
*/
func (b *ringBuffer) ReadAt(
	ctx context.Context, buf []byte, offset int64,
) (int64, int, int64, error) {
	resp, err := readScript.Run(
		ctx, b.core, []string{b.bufferNameKey, b.bufferWrittenKey}, b.capacity, offset, len(buf),
	).Slice()
	if err != nil {
		return -1, 0, 0, goutils.NewRedisError(
			"failed to read from buffer "+b.bufferNameKey, err, true,
		)
	}

	actualOffset := resp[0].(int64)
	total := resp[1].(int64)
	readCount := copy(buf, []byte(resp[2].(string)))

	return actualOffset, readCount, total, nil
}

/*
ReadNewest read the newest bytes from the buffer, filling at most `len(buf)` bytes.

If fewer than `len(buf)` bytes have been written, all available bytes are returned. If
more than the buffer capacity is requested, only the bytes currently in the buffer are
returned.

	@param ctx context.Context - execution context
	@param buf []byte - the receive buffer
	@returns the actual offset of the first byte read
	@returns the number of bytes read
	@returns the total number of bytes written to the buffer
	@returns `REDISError` in case of failure
*/
func (b *ringBuffer) ReadNewest(ctx context.Context, buf []byte) (int64, int, int64, error) {
	resp, err := readNewestScript.Run(
		ctx, b.core, []string{b.bufferNameKey, b.bufferWrittenKey}, b.capacity, len(buf),
	).Slice()
	if err != nil {
		return -1, 0, 0, goutils.NewRedisError(
			"failed to read newest from buffer "+b.bufferNameKey, err, true,
		)
	}

	actualOffset := resp[0].(int64)
	total := resp[1].(int64)
	readCount := copy(buf, []byte(resp[2].(string)))

	return actualOffset, readCount, total, nil
}

/*
Total fetch the total bytes written to the buffer

	@param ctx context.Context - execution context
	@returns the total number of bytes written to the buffer
	@returns `REDISError` in case of failure
*/
func (b *ringBuffer) Total(ctx context.Context) (int64, error) {
	n, err := b.core.Get(ctx, b.bufferWrittenKey).Int64()
	if err == redis.Nil {
		return 0, nil // key not created yet → nothing written
	}
	if err != nil {
		return 0, goutils.NewRedisError(
			"failed to read buffer count key "+b.bufferWrittenKey, err, true,
		)
	}
	return n, nil
}

/*
AsReadWriteCloser create a wrapper around this buffer so it presents an `io.ReadWriteCloser`
interface, allowing this buffer to be used other standard IO helper functions.

	@param parentCtx context.Context - the parent execution context for this buffer
	    which allows for clean execution control of the buffer while maintaining
	    standard interface.
	@param initReadOffset int64 - define the starting read index offset on the buffer
	@param dataCheckInt time.Duration - as the buffer `ReadAt` API is non-blocking,
	    this set the interval of the internal polling loop for available data. Minimum
	    value is 1ms.
	@returns interface complaint with `io.ReadWriteCloser`
*/
func (b *ringBuffer) AsReadWriteCloser(
	parentCtx context.Context, initReadOffset int64, dataCheckInt time.Duration,
) io.ReadWriteCloser {
	instanceCtx, instanceCtxCancel := context.WithCancel(parentCtx)
	return &contextRingBuffer{
		WorkingCtx:       instanceCtx,
		WorkingCtxCancel: instanceCtxCancel,
		Buffer:           b,
		StreamReadPtr:    initReadOffset,
		DataCheckInt:     dataCheckInt,
	}
}

// contextRingBuffer wrapper around `RingBuffer` which implements `io.ReadWriteCloser`
type contextRingBuffer struct {
	WorkingCtx       context.Context
	WorkingCtxCancel context.CancelFunc
	Buffer           RingBuffer

	// StreamReadPtr starting streaming data read pointer
	StreamReadPtr int64
	// DataCheckInt to support the `io.Reader` interface, the `Read` function will loop repeatedly
	// until data is available in the buffer, or working context timed out. Minimum value is 1ms.
	DataCheckInt time.Duration
}

// Close implement the `io.Closer` interface
func (b *contextRingBuffer) Close() error {
	if b.WorkingCtxCancel != nil {
		b.WorkingCtxCancel()
	}
	return nil
}

// Write implement the `io.Writer` interface
func (b *contextRingBuffer) Write(data []byte) (int, error) {
	if err := b.WorkingCtx.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return 0, io.ErrClosedPipe
		}
		return 0, err // surface deadline/other
	}
	written, err := b.Buffer.Write(b.WorkingCtx, data)
	return int(written), err
}

// Read implement the `io.Reader` interface
func (b *contextRingBuffer) Read(buf []byte) (int, error) {
	received := 0

	// No receive buffer
	if len(buf) == 0 {
		return received, nil
	}

	for {
		// Check whether context expired
		if err := b.WorkingCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return received, io.EOF // clean shutdown
			}
			return received, err // surface deadline/other
		}

		actualOffset, readCount, _, err := b.Buffer.ReadAt(b.WorkingCtx, buf, b.StreamReadPtr)
		if err != nil {
			return received, err
		}

		if readCount > 0 {
			b.StreamReadPtr = actualOffset + int64(readCount)
			received = readCount
			break
		}
		// Wait and check again for available data
		waitFor := b.DataCheckInt
		if waitFor < time.Millisecond {
			waitFor = time.Millisecond
		}
		time.Sleep(waitFor)
	}

	return received, nil
}
