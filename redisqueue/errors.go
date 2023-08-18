package redisqueue

type NoNewMessageError string

func (e NoNewMessageError) Error() string { return string(e) }

const NoNewMessage = NoNewMessageError("no new message")

type MessageNotFoundError string

func (e MessageNotFoundError) Error() string { return string(e) }

const MessageNotFound = MessageNotFoundError("no new message")
