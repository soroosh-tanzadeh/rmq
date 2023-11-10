package rmq

type NoNewMessageError string

func (e NoNewMessageError) Error() string { return string(e) }

const NoNewMessage = NoNewMessageError("no new message")

type MessageNotFoundError string

func (e MessageNotFoundError) Error() string { return string(e) }

const MessageNotFound = MessageNotFoundError("no new message")

type NotInitializedError string

func (e NotInitializedError) Error() string { return string(e) }

const NotInitialized = NotInitializedError("you should initialize rmq first")
