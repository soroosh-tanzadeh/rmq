package rmq

// Basic imports
import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
)

type RedisMessageQueueTestSuite struct {
	suite.Suite
	redisServer *miniredis.Miniredis
	ctx         context.Context
	redisClient *redis.Client
}

func TestRedisMessageQueueTestSuite(t *testing.T) {
	suite.Run(t, new(RedisMessageQueueTestSuite))
}

func (s *RedisMessageQueueTestSuite) SetupTest() {
	s.redisServer = miniredis.RunT(s.T())
	s.redisClient = redis.NewClient(&redis.Options{Addr: s.redisServer.Addr()})
	s.ctx = context.Background()
}

func (s *RedisMessageQueueTestSuite) TearDownTest() {
	s.redisServer.FlushAll()
}

func (s *RedisMessageQueueTestSuite) Test_Init_ShouldCreateTheQueue() {
	visibilityTime := rand.Intn(30)
	delayTime := rand.Intn(10)
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", visibilityTime, delayTime, true)

	err := queue.Init(s.ctx)

	s.Require().Nil(err)
	// Check If it adds queue to list
	queues, err := s.redisClient.SMembers(s.ctx, "prefix:QUEUES").Result()
	s.Require().Nil(err)
	s.Contains(queues, "queue")

	// Check if register lua script
	s.Assert().True(s.redisClient.ScriptExists(s.ctx, queue.receiveMessageSha1).Val()[0])
	// Check if it creates queue object
	q, err := s.redisClient.HGetAll(s.ctx, "prefix:queue:Q").Result()
	s.Require().Nil(err)
	s.Assert().Contains(q, "vt")
	s.Assert().Contains(q, "delay")
	s.Assert().Contains(q, "created")
	s.Assert().Contains(q, "modified")
	s.Assert().Equal(fmt.Sprintf("%d", visibilityTime), q["vt"])
	s.Assert().Equal(fmt.Sprintf("%d", delayTime), q["delay"])
	createTime, _ := strconv.Atoi(q["created"])
	modifiedTime, _ := strconv.Atoi(q["modified"])
	s.Assert().True(time.Unix(int64(createTime), 0).Format(time.RFC822) == time.Now().Format(time.RFC822), "Invalid Create time")
	s.Assert().True(time.Unix(int64(modifiedTime), 0).Format(time.RFC822) == time.Now().Format(time.RFC822), "Invalid modified time")
}

func (s *RedisMessageQueueTestSuite) Test_Push_ShouldPushMessageToQueue() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	s.Require().Nil(err)
	payload := "Hello World with random time:" + fmt.Sprintf("%d", time.Now().Unix())

	messageId, err := queue.Push(s.ctx, payload)

	s.Require().Nil(err)
	q, err := s.redisClient.HGetAll(s.ctx, "prefix:queue:Q").Result()
	s.Require().Nil(err)
	s.Assert().Contains(q, messageId)
	s.Assert().Equal(payload, q[messageId], "invalid message payload")
	receiveCMD, _ := s.redisClient.ZRangeByScore(s.ctx, "prefix:queue", &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().UnixNano()), Count: 1, Offset: 0}).Result()
	s.Assert().Equal(messageId, receiveCMD[0], "invalid message id in queue")
	s.Assert().Equal("1", q["totalsent"])
	s.Assert().True(time.UnixMicro(int64(s.redisClient.ZPopMax(s.ctx, "prefix:queue", 1).Val()[0].Score)).Before(time.Now()), "invalid message timestamp in queue")
}

func (s *RedisMessageQueueTestSuite) Test_Push_ShouldReadPushedMessage() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	s.Require().Nil(err)
	payload := "Hello World with random time:" + fmt.Sprintf("%d", time.Now().Unix())
	msgId, err := queue.Push(s.ctx, payload)
	s.Require().Nil(err)

	msg, err := queue.Receive(s.ctx)
	s.Require().Nil(err)

	s.Assert().Equal(payload, msg.GetPayload(), "invalid message payload")
	s.Assert().Equal(msgId, msg.GetId(), "invalid message id")
	s.Assert().Equal(int64(1), msg.GetReceiveCount(), "invalid message receive count")
	s.Assert().Equal(time.Now().Unix(), time.UnixMicro(msg.GetFirstReceive()).Unix(), "invalid first read time")
}

func (s *RedisMessageQueueTestSuite) Test_Push_ShouldMakeMessageInvisibleAfterRead() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	s.Require().Nil(err)
	payload := "Hello World with random time:" + fmt.Sprintf("%d", time.Now().Unix())
	_, err = queue.Push(s.ctx, payload)
	s.Require().Nil(err)
	msg, err := queue.Receive(s.ctx)
	s.Require().NotNil(msg)
	s.Require().Nil(err)

	m, err := queue.Receive(s.ctx)
	s.Equal(Message{}, m)
	s.Require().Equal(NoNewMessage, err)
}

func (s *RedisMessageQueueTestSuite) Test_Push_ConsumeShouldReceiveMessage() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	s.Require().Nil(err)

	messageChannel := make(chan Message)
	go func() {
		err := queue.Consume(s.ctx, func(message Message) {
			messageChannel <- message
		})
		if err != nil {
			s.Error(err)
		}
	}()
	time.Sleep(time.Second)

	payload := "Hello World with random time:" + fmt.Sprintf("%d", time.Now().Unix())
	_, err = queue.Push(s.ctx, payload)
	s.Require().Nil(err)

	timeout, cancel := context.WithTimeout(s.ctx, time.Second)
	select {
	case msg := <-messageChannel:
		s.Assert().Equal(payload, msg.GetPayload())
		cancel()
	case <-timeout.Done():
		s.Fail("Message Not received")
		cancel()
	}
}

func (s *RedisMessageQueueTestSuite) Test_Consume_ShouldReceiveMultipleMessages() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	s.Require().Nil(err)

	messageChannel := make(chan Message)
	go func() {
		err := queue.Consume(s.ctx, func(message Message) {
			messageChannel <- message
			if err := queue.Ack(s.ctx, message.GetId()); err != nil {
				s.Error(err)
			}
		})
		if err != nil {
			s.Error(err)
		}
	}()
	time.Sleep(time.Second)

	msgCount := 100
	payloads := []string{}
	for i := 0; i < msgCount; i++ {
		payload := "Hello World with random time:" + fmt.Sprintf("%d : %d", i, time.Now().Unix())
		_, err = queue.Push(s.ctx, payload)
		payloads = append(payloads, payload)
		s.Require().Nil(err)
	}

	for i := 0; i < msgCount; i++ {
		timeout, cancel := context.WithTimeout(s.ctx, 10*time.Millisecond)
		select {
		case msg := <-messageChannel:
			s.Assert().Equal(payloads[i], msg.GetPayload())
			cancel()
		case <-timeout.Done():
			s.Fail("Message Not received")
			cancel()
		}
	}
}

func (s *RedisMessageQueueTestSuite) Test_Receive_ShouldMultipleProcessShouldNotReadSameMessages() {
	runtime.GOMAXPROCS(10)
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	s.Require().Nil(err)
	pc := make(chan bool, 1)
	go func() {
		for i := 0; i < 100; i++ {
			payload := "Hello World with random time:" + fmt.Sprintf("%d", time.Now().Unix())
			_, err := queue.Push(s.ctx, payload)
			s.Require().Nil(err)
		}
		pc <- true
	}()
	<-pc

	m1 := []string{}
	m2 := []string{}
	d1 := make(chan bool, 1)
	d2 := make(chan bool, 1)
	go func() {
		runtime.LockOSThread()
		for i := 0; i < 100; i++ {
			m, err := queue.Receive(s.ctx)
			if err != NoNewMessage {
				m1 = append(m1, m.GetId())
			}
		}
		d1 <- true
	}()
	go func() {
		runtime.LockOSThread()
		for i := 0; i < 100; i++ {
			m, err := queue.Receive(s.ctx)
			if err != NoNewMessage {
				m2 = append(m2, m.GetId())
			}
		}
		d2 <- true
	}()
	<-d1
	<-d2

	s.Assert().Equal(100, len(m1)+len(m2))
	for _, msgId := range m1 {
		s.Assert().NotContains(msgId, m2)
	}
	for _, msgId := range m2 {
		s.Assert().NotContains(msgId, m1)
	}
}

func (s *RedisMessageQueueTestSuite) Test_Ack_ShouldReturnError_WhenMessageIDIsInvalid() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)

	err = queue.Ack(s.ctx, "invalidID")
	s.Assert().ErrorIs(err, MessageNotFound)
}

func (s *RedisMessageQueueTestSuite) Test_Ack_ShouldRemoveMessageFromHSetAndZSet() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	msgID, err := queue.Push(s.ctx, "RandomPayload")
	s.Require().Nil(err)

	err = queue.Ack(s.ctx, msgID)

	s.Require().Nil(err)
	q, err := s.redisClient.HGetAll(s.ctx, "prefix:queue:Q").Result()
	z, err := s.redisClient.ZPopMax(s.ctx, "prefix:queue").Result()
	s.Require().Nil(err)
	s.Assert().NotContains(q, msgID)
	s.Assert().Len(z, 0)
}

func (s *RedisMessageQueueTestSuite) Test_Ack_NotWorkWhenQueueNotInitialized() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)

	err := queue.Ack(s.ctx, "id")

	s.Assert().ErrorIs(err, NotInitialized)
}

func (s *RedisMessageQueueTestSuite) Test_Push_NotWorkWhenQueueNotInitialized() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)

	_, err := queue.Push(s.ctx, "PayLoad")

	s.Assert().ErrorIs(err, NotInitialized)
}

func (s *RedisMessageQueueTestSuite) Test_Receive_NotWorkWhenQueueNotInitialized() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)

	_, err := queue.Receive(s.ctx)

	s.Assert().ErrorIs(err, NotInitialized)
}

func (s *RedisMessageQueueTestSuite) Test_Consume_NotWorkWhenQueueNotInitialized() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)

	err := queue.Consume(s.ctx, func(message Message) {})

	s.Assert().ErrorIs(err, NotInitialized)
}

func (s *RedisMessageQueueTestSuite) Test_GetMetrics_NotWorkWhenQueueNotInitialized() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)

	_, err := queue.GetMetrics(s.ctx)

	s.Assert().ErrorIs(err, NotInitialized)
}

func (s *RedisMessageQueueTestSuite) Test_GetMetrics() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)
	err := queue.Init(s.ctx)
	s.Require().Nil(err)
	var messages [10]string
	for i := 0; i < 10; i++ {
		if messages[i], err = queue.Push(s.ctx, "Message1"); err != nil {
			s.Error(err)
		}
	}
	_, err = queue.Receive(s.ctx)
	s.Require().Nil(err)

	err = queue.Ack(s.ctx, messages[9])
	s.Require().Nil(err)

	metrics, err := queue.GetMetrics(s.ctx)

	s.Assert().Equal(10, metrics["total_sent"])
	s.Assert().Equal(1, metrics["total_receive"])
	s.Assert().Equal(int64(9), metrics["active_messages"])
}
