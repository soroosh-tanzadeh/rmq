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
	delay          int
	visibilityTime int
	realTime       bool

	queue        string
	prefix       string
	queueHashKey string
	queueKey     string
	listKey      string
	channelKey   string

	initiated          bool
	receiveMessageSha1 string
}

func NewRedisMessageQueue(redisClient *redis.Client, prefix, queue string, visibilityTime, delay int, realTime bool) *RedisMessageQueue {
	rmq := &RedisMessageQueue{
		client:         redisClient,
		queue:          queue,
		queueHashKey:   prefix + ":" + queue + ":Q",
		queueKey:       prefix + ":" + queue,
		listKey:        prefix + ":" + "QUEUES",
		channelKey:     prefix + ":" + queue + "rt:$" + queue,
		delay:          delay,
		visibilityTime: visibilityTime,
		realTime:       realTime,
		initiated:      false,
	}
	return rmq
}

func (r *RedisMessageQueue) Init(ctx context.Context) error {
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

	if r.receiveMessageSha1, err = r.client.ScriptLoad(ctx, receiveMessageScript).Result(); err != nil {
		return err
	}

	createTime, err := r.client.Time(ctx).Result()
	if err != nil {
		return err
	}

	result, err := r.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSetNX(ctx, r.queueHashKey, "vt", strconv.Itoa(r.visibilityTime))
		p.HSetNX(ctx, r.queueHashKey, "delay", strconv.Itoa(r.delay))
		p.HSetNX(ctx, r.queueHashKey, "created", fmt.Sprintf("%d", createTime.Unix()))
		p.HSetNX(ctx, r.queueHashKey, "modified", fmt.Sprintf("%d", createTime.Unix()))
		p.SAdd(ctx, r.listKey, r.queue)
		return nil
	})
	if err != nil {
		return err
	}
	if result[0].Err() == redis.Nil {
		return nil
	}

	r.initiated = true
	return nil
}

func (r *RedisMessageQueue) Push(ctx context.Context, payload string) (string, error) {
	message := rmq.NewMessage(payload, r.queue, 0, 0)

	timestamp, err := r.client.Time(ctx).Result()
	if err != nil {
		return "", err
	}
	txResult, err := r.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZAdd(ctx, r.queueKey, redis.Z{Score: float64(timestamp.UnixMicro() + int64(r.delay*1e+6)), Member: message.GetId()})
		p.HSet(ctx, r.queueHashKey, message.GetId(), message.GetPayload())
		p.HIncrBy(ctx, r.queueHashKey, "totalsent", 1)

		if r.realTime {
			p.ZCard(ctx, r.queueKey)
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	if r.realTime {
		r.client.Publish(ctx, r.channelKey, txResult[3].String())
	}
	return message.GetId(), nil
}

func (r *RedisMessageQueue) Receive(ctx context.Context) (rmq.Message, error) {
	panic("not implemented") // TODO: Implement
}

func (r *RedisMessageQueue) Ack(ctx context.Context, messageId string) error {
	panic("not implemented") // TODO: Implement
}
