package rmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type RedisMessageQueue struct {
	client         *redis.Client
	delay          int
	visibilityTime int
	realTime       bool

	queue        string
	queueHashKey string
	queueKey     string
	listKey      string
	channelKey   string

	initiated          bool
	receiveMessageSha1 string
}

type ConsumeFunc func(message Message)

func NewRedisMessageQueue(redisClient *redis.Client, prefix, queue string, visibilityTime, delay int, realTime bool) *RedisMessageQueue {
	return &RedisMessageQueue{
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
	if errors.Is(result[0].Err(), redis.Nil) {
		return nil
	}

	r.initiated = true
	return nil
}

func (r *RedisMessageQueue) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	if !r.initiated {
		return nil, NotInitialized
	}

	result, err := r.client.HMGet(ctx, r.queueHashKey, "totalsent", "totalrecv").Result()
	if err != nil {
		return nil, err
	}
	activeMessages, err := r.client.ZCard(ctx, r.queueKey).Result()
	if err != nil {
		return nil, err
	}
	totalSent, err := strconv.Atoi(result[0].(string))
	if err != nil {
		return nil, err
	}
	totalReceive, err := strconv.Atoi(result[1].(string))
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"total_sent":      totalSent,
		"total_receive":   totalReceive,
		"active_messages": activeMessages,
	}, nil
}

// Push to queue and returns message ID
func (r *RedisMessageQueue) Push(ctx context.Context, payload string) (string, error) {
	if !r.initiated {
		return "", NotInitialized
	}

	message := NewMessage(uuid.NewString(), payload, r.queue, 0, 0)

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
		msg := txResult[3].(*redis.IntCmd)
		r.client.Publish(ctx, r.channelKey, msg.Val())
	}
	return message.GetId(), nil
}

func (r *RedisMessageQueue) Receive(ctx context.Context) (Message, error) {
	if !r.initiated {
		return Message{}, NotInitialized
	}

	timestamp, err := r.client.Time(ctx).Result()
	if err != nil {
		return Message{}, err
	}
	invisibleUntil := timestamp.Add(time.Second * time.Duration(r.visibilityTime))

	result, err := r.client.EvalSha(ctx, r.receiveMessageSha1, []string{r.queueKey, fmt.Sprintf("%d", timestamp.UnixMicro()), fmt.Sprintf("%d", invisibleUntil.UnixMicro())}, 3).Result()
	if err != nil {
		return Message{}, err
	}
	resp := result.([]interface{})
	if len(resp) == 0 {
		return Message{}, NoNewMessage
	}

	msgId := resp[0].(string)
	payload := resp[1].(string)
	rc := resp[2].(int64)
	fr, err := strconv.ParseInt(resp[3].(string), 10, 64)
	if err != nil {
		return Message{}, err
	}
	return NewMessage(msgId, payload, r.queue, rc, fr), err
}

func (r *RedisMessageQueue) Consume(ctx context.Context, consumer ConsumeFunc) error {
	if !r.initiated {
		return NotInitialized
	}

	channel := r.client.Subscribe(ctx, r.channelKey).Channel()
	for message := range channel {
		cardinality, err := strconv.Atoi(message.Payload)
		if err != nil {
			return err
		}
		if cardinality > 0 {
			for i := 0; i < cardinality; i++ {
				msg, err := r.Receive(ctx)
				if err != nil && !errors.Is(err, NoNewMessage) {
					return err
				} else if errors.Is(err, NoNewMessage) {
					break
				}
				consumer(msg)
			}
		}
	}
	return nil
}

func (r *RedisMessageQueue) Ack(ctx context.Context, messageId string) error {
	if !r.initiated {
		return NotInitialized
	}

	resp, err := r.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, r.queueKey, messageId)
		p.HDel(ctx, r.queueHashKey, messageId, messageId+":ir", messageId+":fr")
		return nil
	})
	if err != nil {
		return err
	}

	if (resp[0].(*redis.IntCmd)).Val() == 0 || (resp[1].(*redis.IntCmd)).Val() == 0 {
		return MessageNotFound
	}

	return nil
}
