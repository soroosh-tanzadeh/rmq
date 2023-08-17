package rmq

import "context"

type MessageQueue interface {
	Push(ctx context.Context, payload string) error
	Receive(ctx context.Context) (Message, error)
	Ack(ctx context.Context, messageId string) error
}
