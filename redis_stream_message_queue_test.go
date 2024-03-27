package rmq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func setupClient() *redis.Client {
	redisServer, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})
	return client
}

func TestRedisStreamMessageQueue(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", 1, true)

	t.Run("Init", func(t *testing.T) {
		err := queue.Init()
		assert.NoError(t, err)
	})

	t.Run("Push", func(t *testing.T) {
		id, err := queue.Push(context.Background(), "test payload")
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

	})

	t.Run("Receive", func(t *testing.T) {
		msg, err := queue.Receive(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "test payload", msg.payload)
	})

	t.Run("Ack", func(t *testing.T) {
		msg, _ := queue.Receive(context.Background())
		err := queue.Ack(context.Background(), "test", msg.id)
		assert.NoError(t, err)
	})

	client.FlushAll(context.Background())
}

func TestRedisStreamMessageQueue_Consume_ShouldCallConsumeFunction(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", 1, true)
	queue.Init()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errorChannel := make(chan error)

	callChannel := make(chan string, 1)
	go func() {
		err := queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(msg Message) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			callChannel <- msg.id
			return nil
		})
		assert.NoError(t, err)
	}()

	id, err := queue.Push(context.Background(), "test payload")
	assert.NoError(t, err)

	select {
	case err := <-errorChannel:
		t.Error(err)
	case actualId := <-callChannel:
		assert.Equal(t, id, actualId)
	case <-time.After(time.Second * 5):
		t.Error("Should Call consume function")
	}
}
