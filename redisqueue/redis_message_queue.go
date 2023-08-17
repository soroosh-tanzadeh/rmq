package redisqueue

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/rmq"
)

type RedisMessageQueue struct {
	client         *redis.Client
	queue          string
	prefix         string
	delay          int
	visibilityTime int
	realTime       bool

	initiated          bool
	receiveMessageSha1 string
}

func NewRedisMessageQueue(redisClient *redis.Client, prefix, queue string, visibilityTime, delay int, realTime bool) *RedisMessageQueue {
	rmq := &RedisMessageQueue{
		client:         redisClient,
		queue:          queue,
		prefix:         prefix + ":",
		delay:          delay,
		visibilityTime: visibilityTime,
		realTime:       realTime,
		initiated:      false,
	}
	return rmq
}

func (rmq *RedisMessageQueue) Init(ctx context.Context) error {
	var err error
	receiveMessageScript := `local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")
	if #msg == 0 then
		return {}
	end
	redis.call("ZADD", KEYS[1], KEYS[3], msg[1])
	redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)
	local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
	local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
	local o = {msg[1], mbody, rc}
	if rc==1 then
		redis.call("HSET", KEYS[1] .. ":Q", msg[1] .. ":fr", KEYS[2])
		table.insert(o, KEYS[2])
	else
		local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")
		table.insert(o, fr)
	end
	return o`

	if rmq.receiveMessageSha1, err = rmq.client.ScriptLoad(ctx, receiveMessageScript).Result(); err != nil {
		return err
	}

	createTime, err := rmq.client.Time(ctx).Result()
	if err != nil {
		return err
	}

	result, err := rmq.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		queueKey := rmq.prefix + rmq.queue + ":Q"

		p.HSetNX(ctx, queueKey, "vt", strconv.Itoa(rmq.visibilityTime))
		p.HSetNX(ctx, queueKey, "delay", strconv.Itoa(rmq.delay))
		p.HSetNX(ctx, queueKey, "created", fmt.Sprintf("%d", createTime.Unix()))
		p.HSetNX(ctx, queueKey, "modified", fmt.Sprintf("%d", createTime.Unix()))
		p.SAdd(ctx, rmq.prefix+"QUEUES", rmq.queue)
		return nil
	})
	if err != nil {
		return err
	}
	if result[0].Err() == redis.Nil {
		return nil
	}

	rmq.initiated = true
	return nil
}

func (rmq *RedisMessageQueue) Push(ctx context.Context, payload string) error {
	panic("not implemented") // TODO: Implement
}

func (rmq *RedisMessageQueue) Receive(ctx context.Context) (rmq.Message, error) {
	panic("not implemented") // TODO: Implement
}

func (rmq *RedisMessageQueue) Ack(ctx context.Context, messageId string) error {
	panic("not implemented") // TODO: Implement
}
