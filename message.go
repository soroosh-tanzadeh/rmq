package rmq

type Message struct {
	queue        string
	id           string
	payload      string
	receiveCount int64
	firstReceive int64
}

func NewMessage(id, payload, queue string, receiveCount, firstReceive int64) Message {
	return Message{
		id:           id,
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

func (m Message) GetReceiveCount() int64 {
	return m.receiveCount
}

func (m Message) GetFirstReceive() int64 {
	return m.firstReceive
}
