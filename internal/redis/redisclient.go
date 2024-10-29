package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/storacha/indexing-service/pkg/redis"
)

type redisValue struct {
	data    string
	expires time.Time
}

type MapRedis struct {
	data map[string]*redisValue
}

var _ redis.Client = (*MapRedis)(nil)

func NewMapRedis() *MapRedis {
	return &MapRedis{data: make(map[string]*redisValue)}
}

func (m *MapRedis) Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd {
	cmd := goredis.NewBoolCmd(ctx, nil)
	val, ok := m.data[key]
	if ok {
		val.expires = time.Now().Add(expiration)
		cmd.SetVal(true)
	}
	return cmd
}

func (m *MapRedis) Get(ctx context.Context, key string) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(ctx, nil)
	val, ok := m.data[key]
	if !ok {
		cmd.SetErr(goredis.Nil)
	} else {
		if !val.expires.IsZero() && val.expires.Before(time.Now()) {
			cmd.SetErr(goredis.Nil)
		} else {
			cmd.SetVal(val.data)
		}
	}
	return cmd
}

func (m *MapRedis) Persist(ctx context.Context, key string) *goredis.BoolCmd {
	cmd := goredis.NewBoolCmd(ctx, nil)
	val, ok := m.data[key]
	if ok && !val.expires.IsZero() {
		val.expires = time.Time{}
		cmd.SetVal(true)
	}
	return cmd
}

func (m *MapRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(ctx, nil)
	var expires time.Time
	if expiration > 0 {
		expires = time.Now().Add(expiration)
	}
	m.data[key] = &redisValue{value.(string), expires}
	return cmd
}
