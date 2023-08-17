package redisqueue

// Basic imports
import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
)

type RedisMessageQueueTestSuite struct {
	suite.Suite
	ctx         context.Context
	redisClient *redis.Client
}

func TestRedisMessageQueueTestSuite(t *testing.T) {
	suite.Run(t, new(RedisMessageQueueTestSuite))
}

func (s *RedisMessageQueueTestSuite) SetupTest() {
	s.redisClient = redis.NewClient(&redis.Options{Addr: miniredis.RunT(s.T()).Addr()})
	s.ctx = context.Background()
}

func (s *RedisMessageQueueTestSuite) Test_Init() {
	queue := NewRedisMessageQueue(s.redisClient, "prefix", "queue", 30, 0, true)

	err := queue.Init(context.Background())

	s.Require().Nil(err)
	// Check If it adds queue to list
	queues, err := s.redisClient.SMembers(s.ctx, "prefix:QUEUES").Result()
	s.Require().Nil(err)
	s.Contains(queues, "queue")
	//Check if it creates queue object
	q, err := s.redisClient.HGetAll(s.ctx, "prefix:queue:Q").Result()
	s.Require().Nil(err)
	s.Assert().Contains(q, "vt")
	s.Assert().Contains(q, "delay")
	s.Assert().Contains(q, "created")
	s.Assert().Contains(q, "modified")
	s.Assert().Equal("30", q["vt"])
	s.Assert().Equal("0", q["delay"])
	createTime, _ := strconv.Atoi(q["created"])
	modifiedTime, _ := strconv.Atoi(q["modified"])
	s.Assert().True(time.Unix(int64(createTime), 0).Format(time.RFC822) == time.Now().Format(time.RFC822), "Invalid Create time")
	s.Assert().True(time.Unix(int64(modifiedTime), 0).Format(time.RFC822) == time.Now().Format(time.RFC822), "Invalid modified time")
}
