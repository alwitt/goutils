package redis

import (
	"context"
	"time"

	"github.com/alwitt/goutils"
	"github.com/redis/go-redis/v9"
)

// QueueMessageEnvelope type erased IPC message object
type QueueMessageEnvelope interface {
	// StringPayload return its payload as a string
	StringPayload() (string, error)
}

// peekRightScript atomically read the right-most (tail) element of a list.
//
// LLEN and LINDEX are evaluated within a single atomic script execution, so a concurrent
// operation that alters the queue length can't slip in between reading the length and
// indexing the tail element.
var peekRightScript = redis.NewScript(
	`-- KEYS[1]=queue
-- Returns: tail element, or false (nil) when the queue is empty
local len = redis.call('LLEN', KEYS[1])
if len == 0 then return false end
return redis.call('LINDEX', KEYS[1], len - 1)`,
)

// pushRightScript atomically RPUSH a message and, when a TTL is supplied, arm an expiry
// on the list key only if it has none yet (NX). Bundling push + PEXPIRE guarantees a reader
// can never observe the key in a TTL-less window after it was first created.
var pushRightScript = redis.NewScript(
	`-- KEYS[1]=queue ; ARGV[1]=payload, ARGV[2]=ttl_epoch_ms (0 = no expiry)
local len = redis.call('RPUSH', KEYS[1], ARGV[1])
local ttl_epoch_ms = tonumber(ARGV[2])
if ttl_epoch_ms > 0 then
  redis.call('PEXPIREAT', KEYS[1], ttl_epoch_ms)
end
return len`,
)

// pushLeftScript — same as pushRightScript but inserts on the left (LPUSH).
var pushLeftScript = redis.NewScript(
	`-- KEYS[1]=queue ; ARGV[1]=payload, ARGV[2]=ttl_epoch_ms (0 = no expiry)
local len = redis.call('LPUSH', KEYS[1], ARGV[1])
local ttl_epoch_ms = tonumber(ARGV[2])
if ttl_epoch_ms > 0 then
  redis.call('PEXPIREAT', KEYS[1], ttl_epoch_ms)
end
return len`,
)

// removeAndMoveScript atomically remove a single occurrence of a specific message from the
// source queue and, only if it was actually present, push it onto the destination queue.
//
// Unlike the position-based *AndMove operations, this targets a message by value, so it is
// safe to use when the source queue holds several unrelated messages (e.g. a reader that has
// staged multiple in-flight messages in its buffer queue). Bundling the LREM and the push in
// one script guarantees the message can never be lost from both queues, nor duplicated onto
// the destination, if the caller crashes mid-operation.
//
// The removal count is returned so the caller can tell "moved" (1) from "message was not in
// the source queue" (0); on 0 nothing is pushed, so a caller can never fabricate a message
// that did not originate from the source queue.
//
// NOTE: KEYS[1] and KEYS[2] must live on the same Redis node. On a single-node deployment
// (as used here) this always holds; on Redis Cluster the two queues would need a shared hash
// tag to be co-located.
var removeAndMoveScript = redis.NewScript(
	`-- KEYS[1]=source, KEYS[2]=destination ; ARGV[1]=payload, ARGV[2]=dest_on_left (1=LPUSH)
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then return 0 end
if tonumber(ARGV[2]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[1])
else
  redis.call('RPUSH', KEYS[2], ARGV[1])
end
return removed`,
)

// queueMessage REDIS queue message are all represented as strings
type queueMessage string

// StringPayload return its payload as a string
func (m queueMessage) StringPayload() (string, error) {
	return string(m), nil
}

// Queue REDIS queue manager
type Queue interface {
	// QueueName get the REDIS queue name
	QueueName() string

	/*
		PushRight push message into queue right

			@param ctx context.Context - execution context
			@param message IPCMessageEnvelope - message to insert
			@param ttl time.Duration - if > 0, optionally set a TTL for the message.
			@return length of queue after insert
	*/
	PushRight(
		ctx context.Context, message QueueMessageEnvelope, ttl *time.Duration,
	) (uint64, error)

	/*
		PushLeft push message into queue left

			@param ctx context.Context - execution context
			@param message IPCMessageEnvelope - message to insert
			@param ttl time.Duration - if > 0, optionally set a TTL for the message.
			@return length of queue after insert
	*/
	PushLeft(
		ctx context.Context, message QueueMessageEnvelope, ttl *time.Duration,
	) (uint64, error)

	/*
		PopRight pop message from queue right

			@param ctx context.Context - execution context
			@param blocking bool - whether this is a blocking operation
			@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
			@return message or nil if empty
	*/
	PopRight(
		ctx context.Context, blocking bool, maxWait *time.Duration,
	) (QueueMessageEnvelope, error)

	/*
		PopLeft pop message from queue left

			@param ctx context.Context - execution context
			@param blocking bool - whether this is a blocking operation
			@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
			@return message or nil if empty
	*/
	PopLeft(
		ctx context.Context, blocking bool, maxWait *time.Duration,
	) (QueueMessageEnvelope, error)

	/*
		PopLeftAndMove pop message from queue left and move that message to another queue.

		On completion, return the popped message.

			@param ctx context.Context - execution context
			@param destination string - destination queue
			@param insertOnLeft bool - whether to insert on left of destination queue. Default is right.
			@param blocking bool - whether this is a blocking operation
			@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
			@return message or nil if empty
	*/
	PopLeftAndMove(
		ctx context.Context,
		destination string,
		insertOnLeft bool,
		blocking bool,
		maxWait *time.Duration,
	) (QueueMessageEnvelope, error)

	/*
		PopRightAndMove pop message from queue right and move that message to another queue.

		On completion, return the popped message.

			@param ctx context.Context - execution context
			@param destination string - destination queue
			@param insertOnLeft bool - whether to insert on left of destination queue. Default is right.
			@param blocking bool - whether this is a blocking operation
			@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
			@return message or nil if empty
	*/
	PopRightAndMove(
		ctx context.Context,
		destination string,
		insertOnLeft bool,
		blocking bool,
		maxWait *time.Duration,
	) (QueueMessageEnvelope, error)

	/*
		RemoveAndMove atomically remove a single occurrence of a specific message from this
		queue and, only if it was present, push it onto the destination queue.

		Unlike PopLeftAndMove / PopRightAndMove, the message is targeted by value rather than
		by position, so this is safe to use when this queue holds several unrelated messages.
		The remove and push happen in one atomic step, so the message can never be lost from
		both queues nor duplicated onto the destination if the caller crashes mid-operation.

		If the message is not present in this queue nothing is moved and an error is returned;
		this prevents a caller from injecting a message that did not originate from this queue.

			@param ctx context.Context - execution context
			@param destination string - destination queue
			@param message QueueMessageEnvelope - the specific message to move
			@param insertOnLeft bool - whether to insert on left of destination queue. Default is right.
	*/
	RemoveAndMove(
		ctx context.Context,
		destination string,
		message QueueMessageEnvelope,
		insertOnLeft bool,
	) error

	/*
		PeakLeft read the left most value of the queue without removing it

			@param ctx context.Context - execution context
			@return message or nil if empty
	*/
	PeakLeft(ctx context.Context) (QueueMessageEnvelope, error)

	/*
		PeakRight read the right most (tail) value of the queue without removing it

			@param ctx context.Context - execution context
			@return message or nil if empty
	*/
	PeakRight(ctx context.Context) (QueueMessageEnvelope, error)

	/*
		Length fetch the length of the queue

			@param ctx context.Context - execution context
			@return length of queue
	*/
	Length(ctx context.Context) (uint64, error)

	/*
		Remove delete a message from the queue

			@param ctx context.Context - execution context
			@param message IPCMessageEnvelope - the message to delete
	*/
	Remove(ctx context.Context, message QueueMessageEnvelope) error
}

type redisQueueImpl struct {
	goutils.Component
	queueName string
	core      *redis.Client
}

// QueueName get the REDIS queue name
func (q *redisQueueImpl) QueueName() string {
	return q.queueName
}

/*
PushRight push message into queue right

	@param ctx context.Context - execution context
	@param message IPCMessageEnvelope - message to insert
	@param ttl time.Duration - if > 0, optionally set a TTL for the message.
	@return length of queue after insert
*/
func (q *redisQueueImpl) PushRight(
	ctx context.Context, message QueueMessageEnvelope, ttl *time.Duration,
) (uint64, error) {
	toInsert, err := message.StringPayload()
	if err != nil {
		return 0, goutils.NewBadInputError("queue message failed to serialize", err, true)
	}

	ttlEpochMS := int64(0)
	if ttl != nil && ttl.Abs() > 0 {
		ttlEpochMS = time.Now().UTC().Add(*ttl).UnixMilli()
	}

	length, err := pushRightScript.Run(
		ctx, q.core, []string{q.queueName}, toInsert, ttlEpochMS,
	).Uint64()
	if err != nil {
		return 0, goutils.NewRedisError("failed to push right on queue "+q.queueName, err, true)
	}

	return length, nil
}

/*
PushLeft push message into queue left

	@param ctx context.Context - execution context
	@param message IPCMessageEnvelope - message to insert
	@param ttl time.Duration - if > 0, optionally set a TTL for the message.
	@return length of queue after insert
*/
func (q *redisQueueImpl) PushLeft(
	ctx context.Context, message QueueMessageEnvelope, ttl *time.Duration,
) (uint64, error) {
	toInsert, err := message.StringPayload()
	if err != nil {
		return 0, goutils.NewBadInputError("queue message failed to serialize", err, true)
	}

	ttlEpochMS := int64(0)
	if ttl != nil && ttl.Abs() > 0 {
		ttlEpochMS = time.Now().UTC().Add(*ttl).UnixMilli()
	}

	length, err := pushLeftScript.Run(
		ctx, q.core, []string{q.queueName}, toInsert, ttlEpochMS,
	).Uint64()
	if err != nil {
		return 0, goutils.NewRedisError("failed to push right on queue "+q.queueName, err, true)
	}

	return length, nil
}

/*
PopRight pop message from queue right

	@param ctx context.Context - execution context
	@param blocking bool - whether this is a blocking operation
	@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
	@return message or nil if empty
*/
func (q *redisQueueImpl) PopRight(
	ctx context.Context, blocking bool, maxWait *time.Duration,
) (QueueMessageEnvelope, error) {
	// Blocking read
	if blocking {
		waitTime := time.Second * 5
		if maxWait != nil {
			waitTime = *maxWait
		}
		resp := q.core.BRPop(ctx, waitTime, q.queueName)
		if resp.Err() != nil {
			if resp.Err() == redis.Nil {
				return nil, nil
			}
			return nil, goutils.NewRedisError(
				"failed to blocking pop queue "+q.queueName+" right", resp.Err(), true,
			)
		}
		allReturns := resp.Val()
		if len(allReturns) != 2 {
			return nil, goutils.NewConsistencyError(
				"blocking pop queue "+q.queueName+" right response wrong shape", nil, true,
			)
		}
		return queueMessage(allReturns[1]), nil
	}

	// Non-blocking read
	resp := q.core.RPop(ctx, q.queueName)
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return nil, nil
		}
		return nil, goutils.NewRedisError(
			"failed to pop queue "+q.queueName+" right", resp.Err(), true,
		)
	}

	return queueMessage(resp.Val()), nil
}

/*
PopLeft pop message from queue left

	@param ctx context.Context - execution context
	@param blocking bool - whether this is a blocking operation
	@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
	@return message or nil if empty
*/
func (q *redisQueueImpl) PopLeft(
	ctx context.Context, blocking bool, maxWait *time.Duration,
) (QueueMessageEnvelope, error) {
	// Blocking read
	if blocking {
		waitTime := time.Second * 5
		if maxWait != nil {
			waitTime = *maxWait
		}
		resp := q.core.BLPop(ctx, waitTime, q.queueName)
		if resp.Err() != nil {
			if resp.Err() == redis.Nil {
				return nil, nil
			}
			return nil, goutils.NewRedisError(
				"failed to blocking pop queue "+q.queueName+" left", resp.Err(), true,
			)
		}
		allReturns := resp.Val()
		if len(allReturns) != 2 {
			return nil, goutils.NewConsistencyError(
				"blocking pop queue "+q.queueName+" left response wrong shape", nil, true,
			)
		}
		return queueMessage(allReturns[1]), nil
	}

	// Non-blocking read
	resp := q.core.LPop(ctx, q.queueName)
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return nil, nil
		}
		return nil, goutils.NewRedisError(
			"failed to pop queue "+q.queueName+" left", resp.Err(), true,
		)
	}

	return queueMessage(resp.Val()), nil
}

/*
PopLeftAndMove pop message from queue left and move that message to another queue.

On completion, return the popped message.

	@param ctx context.Context - execution context
	@param destination string - destination queue
	@param insertOnLeft bool - whether to insert on left of destination queue. Default is right.
	@param blocking bool - whether this is a blocking operation
	@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
	@return message or nil if empty
*/
func (q *redisQueueImpl) PopLeftAndMove(
	ctx context.Context,
	destination string,
	insertOnLeft bool,
	blocking bool,
	maxWait *time.Duration,
) (QueueMessageEnvelope, error) {
	const sourcePos = "LEFT"
	destPos := "RIGHT"
	if insertOnLeft {
		destPos = "LEFT"
	}

	// Blocking pop-and-move
	if blocking {
		waitTime := time.Second * 5
		if maxWait != nil {
			waitTime = *maxWait
		}
		resp := q.core.BLMove(ctx, q.queueName, destination, sourcePos, destPos, waitTime)
		if resp.Err() != nil {
			if resp.Err() == redis.Nil {
				return nil, nil
			}
			return nil, goutils.NewRedisError(
				"failed to blocking pop-move queue "+
					q.queueName+
					" "+
					sourcePos+
					" to queue "+
					destination+
					" "+
					destPos,
				resp.Err(),
				true,
			)
		}
		return queueMessage(resp.Val()), nil
	}

	// Non-blocking pop-and-mov
	resp := q.core.LMove(ctx, q.queueName, destination, sourcePos, destPos)
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return nil, nil
		}
		return nil, goutils.NewRedisError(
			"failed to pop-move queue "+
				q.queueName+
				" "+
				sourcePos+
				" to queue "+
				destination+
				" "+
				destPos,
			resp.Err(),
			true,
		)
	}

	return queueMessage(resp.Val()), nil
}

/*
PopRightAndMove pop message from queue right and move that message to another queue.

On completion, return the popped message.

	@param ctx context.Context - execution context
	@param destination string - destination queue
	@param insertOnLeft bool - whether to insert on left of destination queue. Default is right.
	@param blocking bool - whether this is a blocking operation
	@param maxWait *time.Duration - if blocking, the max duration. Default 5 sec.
	@return message or nil if empty
*/
func (q *redisQueueImpl) PopRightAndMove(
	ctx context.Context,
	destination string,
	insertOnLeft bool,
	blocking bool,
	maxWait *time.Duration,
) (QueueMessageEnvelope, error) {
	const sourcePos = "RIGHT"
	destPos := "RIGHT"
	if insertOnLeft {
		destPos = "LEFT"
	}

	// Blocking pop-and-move
	if blocking {
		waitTime := time.Second * 5
		if maxWait != nil {
			waitTime = *maxWait
		}
		resp := q.core.BLMove(ctx, q.queueName, destination, sourcePos, destPos, waitTime)
		if resp.Err() != nil {
			if resp.Err() == redis.Nil {
				return nil, nil
			}
			return nil, goutils.NewRedisError(
				"failed to blocking pop-move queue "+
					q.queueName+
					" "+
					sourcePos+
					" to queue "+
					destination+
					" "+
					destPos,
				resp.Err(),
				true,
			)
		}
		return queueMessage(resp.Val()), nil
	}

	// Non-blocking pop-and-mov
	resp := q.core.LMove(ctx, q.queueName, destination, sourcePos, destPos)
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return nil, nil
		}
		return nil, goutils.NewRedisError(
			"failed to pop-move queue "+
				q.queueName+
				" "+
				sourcePos+
				" to queue "+
				destination+
				" "+
				destPos,
			resp.Err(),
			true,
		)
	}

	return queueMessage(resp.Val()), nil
}

/*
RemoveAndMove atomically remove a single occurrence of a specific message from this queue
and, only if it was present, push it onto the destination queue.

Unlike PopLeftAndMove / PopRightAndMove, the message is targeted by value rather than by
position, so this is safe to use when this queue holds several unrelated messages. The remove
and push happen in one atomic LUA step, so the message can never be lost from both queues nor
duplicated onto the destination if the caller crashes mid-operation.

If the message is not present in this queue nothing is moved and an error is returned; this
prevents a caller from injecting a message that did not originate from this queue.

	@param ctx context.Context - execution context
	@param destination string - destination queue
	@param message QueueMessageEnvelope - the specific message to move
	@param insertOnLeft bool - whether to insert on left of destination queue. Default is right.
*/
func (q *redisQueueImpl) RemoveAndMove(
	ctx context.Context,
	destination string,
	message QueueMessageEnvelope,
	insertOnLeft bool,
) error {
	payload, err := message.StringPayload()
	if err != nil {
		return goutils.NewBadInputError("queue message failed to serialize", err, true)
	}

	destOnLeft := 0
	if insertOnLeft {
		destOnLeft = 1
	}

	removed, err := removeAndMoveScript.Run(
		ctx, q.core, []string{q.queueName, destination}, payload, destOnLeft,
	).Int64()
	if err != nil {
		return goutils.NewRedisError(
			"failed to remove-move queue "+q.queueName+" to queue "+destination, err, true,
		)
	}

	if removed == 0 {
		return goutils.NewConsistencyError(
			"message '"+payload+"' not present in queue "+q.queueName+"; nothing moved to "+
				destination,
			nil,
			true,
		)
	}

	return nil
}

/*
PeakLeft read the left most value of the queue without removing it

	@param ctx context.Context - execution context
	@return message or nil if empty
*/
func (q *redisQueueImpl) PeakLeft(ctx context.Context) (QueueMessageEnvelope, error) {
	resp := q.core.LIndex(ctx, q.queueName, 0)
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return nil, nil
		}
		return nil, goutils.NewRedisError(
			"failed to peak queue "+q.queueName+" left", resp.Err(), true,
		)
	}
	return queueMessage(resp.Val()), nil
}

/*
PeakRight read the right most (tail) value of the queue without removing it.

The length and tail lookup are performed atomically via a LUA script, so a concurrent
operation that changes the queue length can't race between reading the length and
indexing the tail element.

	@param ctx context.Context - execution context
	@return message or nil if empty
*/
func (q *redisQueueImpl) PeakRight(ctx context.Context) (QueueMessageEnvelope, error) {
	resp := peekRightScript.Run(ctx, q.core, []string{q.queueName})
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return nil, nil
		}
		return nil, goutils.NewRedisError(
			"failed to peak queue "+q.queueName+" right", resp.Err(), true,
		)
	}

	// An empty queue returns a Lua `false`, which the client surfaces as redis.Nil.
	val, err := resp.Text()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, goutils.NewConsistencyError(
			"right peaking queue "+q.queueName+" returned unexpected value", err, true,
		)
	}

	return queueMessage(val), nil
}

/*
Length fetch the length of the queue

	@param ctx context.Context - execution context
	@return length of queue
*/
func (q *redisQueueImpl) Length(ctx context.Context) (uint64, error) {
	resp := q.core.LLen(ctx, q.queueName)
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return 0, nil
		}
		return 0, goutils.NewRedisError(
			"failed to get queue "+q.queueName+" length", resp.Err(), true,
		)
	}

	length, err := resp.Uint64()
	if err != nil {
		return 0, goutils.NewConsistencyError(
			"queue "+q.queueName+" length not returned", err, true,
		)
	}

	return length, nil
}

/*
Remove delete a message from the queue

	@param ctx context.Context - execution context
	@param message IPCMessageEnvelope - the message to delete
*/
func (q *redisQueueImpl) Remove(ctx context.Context, message QueueMessageEnvelope) error {
	toDelete, err := message.StringPayload()
	if err != nil {
		return goutils.NewBadInputError("queue message failed to serialize", err, true)
	}

	resp := q.core.LRem(ctx, q.queueName, 0, toDelete)
	if resp.Err() != nil {
		if resp.Err() == redis.Nil {
			return nil
		}
		return goutils.NewRedisError(
			"failed to delete from queue "+q.queueName, resp.Err(), true,
		)
	}
	// A message that is not present is treated as a no-op success: removing something that is
	// already gone leaves the queue in the intended state, and callers (e.g. an at-most-once
	// ack) must not see a spurious error when the message was already handled or expired.

	return nil
}
