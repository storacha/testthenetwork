package redis

import (
	"context"
	"maps"
	"slices"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/storacha/indexing-service/pkg/redis"
)

type redisValue struct {
	data    map[string]struct{}
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
			for k := range val.data {
				cmd.SetVal(k)
				break
			}
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
	data := map[string]struct{}{value.(string): {}}
	m.data[key] = &redisValue{data, expires}
	return cmd
}

func (m *MapRedis) SAdd(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx, nil)
	data := map[string]struct{}{}
	expires := time.Time{}
	val, ok := m.data[key]
	if ok {
		data = val.data
		expires = val.expires
	}
	written := uint64(0)
	for _, v := range values {
		_, ok := data[v.(string)]
		if !ok {
			data[v.(string)] = struct{}{}
			written++
		}
	}
	m.data[key] = &redisValue{data, expires}
	cmd.SetVal(int64(written))
	return cmd
}

func (m *MapRedis) SMembers(ctx context.Context, key string) *goredis.StringSliceCmd {
	cmd := goredis.NewStringSliceCmd(ctx, nil)
	val, ok := m.data[key]
	if !ok {
		cmd.SetErr(goredis.Nil)
	} else {
		values := slices.Collect(maps.Keys(val.data))
		cmd.SetVal(values)
	}
	return cmd
}
