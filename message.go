package rmq

import "github.com/google/uuid"

type Message struct {
	queue        string
	id           string
	payload      string
	receiveCount int
	firstReceive int
}

func NewMessage(payload, queue string, receiveCount, firstReceive int) Message {
	return Message{
		id:           uuid.NewString(),
		payload:      payload,
		queue:        queue,
		receiveCount: receiveCount,
		firstReceive: firstReceive,
	}
}

func (m Message) GetQueue() string {
	return m.queue
}

func (m Message) GetId() string {
	return m.id
}

func (m Message) GetPayload() string {
	return m.payload
}

func (m Message) GetReceiveCount() int {
	return m.receiveCount
}

func (m Message) GetFirstReceive() int {
	return int(m.firstReceive)
}
