package rmq

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStreamMessageQueue struct {
	client   *redis.Client
	queueKey string

	consumerGroupScriptSha string

	deleteOnConsume bool
	initiated       bool
}

func NewRedisStreamMessageQueue(redisClient *redis.Client, prefix, queue string, claimDelay int, deleteOnConsume bool) *RedisStreamMessageQueue {
	return &RedisStreamMessageQueue{
		client:          redisClient,
		queueKey:        prefix + ":" + queue,
		deleteOnConsume: deleteOnConsume,
		initiated:       false,
	}
}

func (r *RedisStreamMessageQueue) Init() error {
	consumerGroupScript := `local stream_name = ARGV[1]
	local group_name = ARGV[2]
	local consumer_name = ARGV[3]
	
	local stream_exists = redis.call("EXISTS", stream_name)
	if stream_exists == 0 then
	return { error = "Stream '" .. stream_name .. "' does not exist." }
	end
	
	redis.call("XGROUP", "CREATE", stream_name, group_name, "$")
	
	return { message = "Consumer group '" .. group_name .. "' created for stream '" .. stream_name .. "'" }`
	var err error

	if r.consumerGroupScriptSha, err = r.client.ScriptLoad(context.Background(), consumerGroupScript).Result(); err != nil {
		return err
	}

	r.initiated = true

	return nil
}

func (r *RedisStreamMessageQueue) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	// TODO
	return nil, nil
}

// Push to queue and returns message ID
func (r *RedisStreamMessageQueue) Push(ctx context.Context, payload string) (string, error) {
	return r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.queueKey,
		Values: map[string]interface{}{
			"retry_count": 0,
			"payload":     payload,
		},
	}).Result()
}

func (r *RedisStreamMessageQueue) Receive(ctx context.Context) (Message, error) {
	msg, err := r.client.XRead(ctx, &redis.XReadArgs{
		Block:   time.Second,
		Count:   1,
		Streams: []string{r.queueKey, "0-0"},
	}).Result()
	if err != nil {
		return Message{}, err
	}

	if len(msg) == 0 {
		return Message{}, ErrNoNewMessage
	}

	if len(msg[0].Messages) == 0 {
		return Message{}, ErrNoNewMessage
	}

	values := msg[0].Messages[0].Values
	id := msg[0].Messages[0].ID
	retry_count, err := strconv.Atoi(values["retry_count"].(string))
	if err != nil {
		return Message{}, err
	}
	message := Message{
		id:           id,
		queue:        r.queueKey,
		payload:      values["payload"].(string),
		receiveCount: int64(retry_count),
	}
	return message, nil
}

func (r *RedisStreamMessageQueue) upsertConsumerGroup(group string) error {
	if !r.initiated {
		return ErrNotInitialized
	}

	_, err := r.client.XGroupCreate(context.Background(), r.queueKey, group, "0-0").Result()
	if err != nil {
		if strings.Compare(err.Error(), "BUSYGROUP Consumer Group name already exists") == 0 {
			return nil
		}
		return err
	}

	return nil
}

func (r *RedisStreamMessageQueue) Consume(ctx context.Context, readBatchSize int, blockDuration time.Duration, group, consumerName string, errorChannel chan error, consumer ConsumeFunc) error {
	if err := r.upsertConsumerGroup(group); err != nil {
		return err
	}

	for {
		// Return immediately if ctx is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		readResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumerName,
			Count:    int64(readBatchSize),
			Streams:  []string{r.queueKey, ">"},
			Block:    blockDuration,
		}).Result()
		if err != nil {
			errorChannel <- err
			continue
		}

		// Check if is there any new message or not
		if len(readResult) == 0 {
			continue
		}
		if len(readResult[0].Messages) == 0 {
			continue
		}

		// Consume Messages
		for _, msg := range readResult[0].Messages {
			values := msg.Values
			id := msg.ID
			retry_count, err := strconv.Atoi(values["retry_count"].(string))
			if err != nil {
				errorChannel <- err
				continue
			}

			// Should not execute if context is canceled
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			message := Message{
				id:           id,
				queue:        r.queueKey,
				payload:      values["payload"].(string),
				receiveCount: int64(retry_count),
			}
			retry_count++

			if err := consumer(message); err != nil {
				errorChannel <- err
				continue
			}

			if err := r.Ack(ctx, group, id); err != nil {
				errorChannel <- err
			}

			if r.deleteOnConsume {
				if err := r.client.XDel(ctx, r.queueKey, id).Err(); err != nil {
					errorChannel <- err
				}
			}
		}
	}
}

func (r *RedisStreamMessageQueue) Ack(ctx context.Context, group, messageId string) error {
	_, err := r.client.XAck(ctx, r.queueKey, group, messageId).Result()
	return err
}
