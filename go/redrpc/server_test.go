package redrpc

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stvp/assert"
)

type TestDbAdapter struct {
	blpopMock   func(timeout time.Duration, keys ...string) ([]string, error)
	rpushexMock func(key, value string, ttl time.Duration) error
}

func (a *TestDbAdapter) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	return a.blpopMock(timeout, keys...)
}

func (a *TestDbAdapter) RPushEx(key, value string, ttl time.Duration) error {
	return a.rpushexMock(key, value, ttl)
}

func TestNoCalls(t *testing.T) {
	red := &TestDbAdapter{
		blpopMock: func(timeout time.Duration, keys ...string) ([]string, error) {
			return []string{}, redis.Nil
		},
	}
	srv := NewServerWithAdapter(red, nil, map[string]Handler{})
	assert.True(t, srv.RunOnce())
}

func TestErrorOnBLPop(t *testing.T) {
	red := &TestDbAdapter{
		blpopMock: func(timeout time.Duration, keys ...string) ([]string, error) {
			return []string{}, fmt.Errorf("Error in blpop")
		},
	}
	srv := NewServerWithAdapter(red, nil, map[string]Handler{})
	assert.False(t, srv.RunOnce())
}

func TestCorrectCall(t *testing.T) {
	var result []string
	var resultTTL time.Duration

	red := &TestDbAdapter{
		blpopMock: func(timeout time.Duration, keys ...string) ([]string, error) {
			return []string{
				"redis_rpc:test:calls",
				`{"id":"call-id", "ts":"2017-01-01T00:00:00Z", "kw":{"i": 123, "s": "str123"}}`,
			}, nil
		},
		rpushexMock: func(key, value string, ttl time.Duration) error {
			result = []string{key, value}
			resultTTL = ttl
			return nil
		},
	}
	opts := &Options{
		ResultExpire: 10 * time.Second,
		TimeSource:   func() time.Time { t, _ := time.Parse(time.RFC3339, "2018-01-01T00:00:00Z"); return t },
	}
	srv := NewServerWithAdapter(red, opts, map[string]Handler{
		"test": HandlerFunc(func(req Request) (interface{}, error) {
			assert.Equal(t, req.GetValue("i"), 123.0)
			assert.Equal(t, req.GetValue("s"), "str123")
			return "the-result", nil
		}),
	})
	assert.True(t, srv.RunOnce())
	assert.Equal(t, result, []string{"redis_rpc:test:result:call-id", `{"ts":"2018-01-01T00:00:00Z","res":"the-result"}`})
	assert.Equal(t, resultTTL, 10*time.Second)
}

func TestPropagateError(t *testing.T) {
	var result []string
	var resultTTL time.Duration

	red := &TestDbAdapter{
		blpopMock: func(timeout time.Duration, keys ...string) ([]string, error) {
			return []string{"redis_rpc:test:calls", `{"id":"call-id", "ts":"2017-01-01T00:00:00Z", "kw":{}}`}, nil
		},
		rpushexMock: func(key, value string, ttl time.Duration) error {
			result = []string{key, value}
			resultTTL = ttl
			return nil
		},
	}
	opts := &Options{
		ResultExpire: 10 * time.Second,
		TimeSource:   func() time.Time { t, _ := time.Parse(time.RFC3339, "2018-01-01T00:00:00Z"); return t },
	}
	srv := NewServerWithAdapter(red, opts, map[string]Handler{
		"test": HandlerFunc(func(req Request) (interface{}, error) {
			return nil, fmt.Errorf("test error")
		}),
	})
	assert.True(t, srv.RunOnce())
	assert.Equal(t, result, []string{"redis_rpc:test:result:call-id", `{"ts":"2018-01-01T00:00:00Z","err":"test error"}`})
	assert.Equal(t, resultTTL, 10*time.Second)
}

func TestPropagatePanic(t *testing.T) {
	var result []string
	var resultTTL time.Duration

	red := &TestDbAdapter{
		blpopMock: func(timeout time.Duration, keys ...string) ([]string, error) {
			return []string{"redis_rpc:test:calls", `{"id":"call-id", "ts":"2017-01-01T00:00:00Z", "kw":{}}`}, nil
		},
		rpushexMock: func(key, value string, ttl time.Duration) error {
			result = []string{key, value}
			resultTTL = ttl
			return nil
		},
	}
	opts := &Options{
		ResultExpire: 10 * time.Second,
		TimeSource:   func() time.Time { t, _ := time.Parse(time.RFC3339, "2018-01-01T00:00:00Z"); return t },
	}
	srv := NewServerWithAdapter(red, opts, map[string]Handler{
		"test": HandlerFunc(func(req Request) (interface{}, error) {
			panic("oh no")
		}),
	})
	assert.True(t, srv.RunOnce())
	assert.Equal(t, result, []string{"redis_rpc:test:result:call-id", `{"ts":"2018-01-01T00:00:00Z","err":"oh no"}`})
	assert.Equal(t, resultTTL, 10*time.Second)
}

func TestServerRotatesQueues(t *testing.T) {
	blpopCalls := [][]string{}

	red := &TestDbAdapter{
		blpopMock: func(timeout time.Duration, keys ...string) ([]string, error) {
			blpopCalls = append(blpopCalls, keys)
			return nil, nil
		},
	}
	srv := NewServerWithAdapter(red, nil, map[string]Handler{
		"a": HandlerFunc(func(req Request) (interface{}, error) { return nil, nil }),
		"b": HandlerFunc(func(req Request) (interface{}, error) { return nil, nil }),
		"c": HandlerFunc(func(req Request) (interface{}, error) { return nil, nil }),
	})
	assert.True(t, srv.RunOnce())
	assert.True(t, srv.RunOnce())
	assert.True(t, srv.RunOnce())

	assert.Equal(t, blpopCalls, [][]string{
		[]string{"redis_rpc:a:calls", "redis_rpc:b:calls", "redis_rpc:c:calls"},
		[]string{"redis_rpc:b:calls", "redis_rpc:c:calls", "redis_rpc:a:calls"},
		[]string{"redis_rpc:c:calls", "redis_rpc:a:calls", "redis_rpc:b:calls"},
	})
}
