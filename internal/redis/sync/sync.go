package sync

import (
	"context"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/storacha/indexing-service/pkg/redis"
)

type MutexRedis struct {
	sync.RWMutex
	child redis.Client
}

func (m *MutexRedis) Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd {
	m.Lock()
	defer m.Unlock()
	return m.child.Expire(ctx, key, expiration)
}

func (m *MutexRedis) Get(ctx context.Context, key string) *goredis.StringCmd {
	m.RLock()
	defer m.RUnlock()
	return m.child.Get(ctx, key)
}

func (m *MutexRedis) Persist(ctx context.Context, key string) *goredis.BoolCmd {
	m.Lock()
	defer m.Unlock()
	return m.child.Persist(ctx, key)
}

func (m *MutexRedis) Set(ctx context.Context, key string, value any, expiration time.Duration) *goredis.StatusCmd {
	m.Lock()
	defer m.Unlock()
	return m.child.Set(ctx, key, value, expiration)
}

func (m *MutexRedis) SAdd(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd {
	m.Lock()
	defer m.Unlock()
	return m.child.SAdd(ctx, key, values...)
}

func (m *MutexRedis) SMembers(ctx context.Context, key string) *goredis.StringSliceCmd {
	m.RLock()
	defer m.RUnlock()
	return m.child.SMembers(ctx, key)
}

var _ redis.Client = (*MutexRedis)(nil)

func MutexWrap(c redis.Client) *MutexRedis {
	return &MutexRedis{child: c}
}
