# RMQ
RMQ (stands for Redis Message Queue) is a simple message queue that allows communication between applications through Redis ZSet.
RMQ uses micro-time to prevent duplicated message delivery.

## Usages
```go
package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/rmq"
	"github.com/soroosh-tanzadeh/rmq/redisqueue"
)

func main() {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	visibilityTime := 100
	queue := redisqueue.NewRedisMessageQueue(client, "prefix", "queue", visibilityTime, delayTime, true)

	// Initialize Message Queue
	err := queue.Init(ctx)
	if err != nil {
		panic(err)
	}

	// Push message to queue
	messageId, err := queue.Push(ctx, "Message Payload")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Message with id %s pushed to queue", messageId)

	// Consume Messages
	go func() {
		err := queue.Consume(ctx, func(message rmq.Message) {
			fmt.Printf("Message with id %s received from queue", message.GetId())
		})
		if err != nil {
			panic(err)
		}
	}()
}
```