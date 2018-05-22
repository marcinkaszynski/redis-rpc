package redrpc

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

//////////////////////////////////////////////////
// Handler

type Handler interface {
	ServeRPC(Request) (interface{}, error)
}

type HandlerFunc func(Request) (interface{}, error)

func (h HandlerFunc) ServeRPC(r Request) (interface{}, error) {
	return h(r)
}

//////////////////////////////////////////////////
// Request

type Request interface {
	GetValue(name string) interface{}
	GetString(name string) string
}

//////////////////////////////////////////////////
// Server

type Server struct {
	red      DbAdapter
	opts     *Options
	handlers map[string]Handler
	queues   []string
	queueMap map[string]string

	closing    int64
	iterations int
}

type RequestImpl struct {
	Id string                 `json:"id"`
	Ts string                 `json:"ts"`
	Kw map[string]interface{} `json:"kw"`
}

func (r RequestImpl) GetValue(k string) interface{} {
	return r.Kw[k]
}

func (r RequestImpl) GetString(k string) string {
	v := r.GetValue(k)
	if v == nil {
		return ""
	}

	str, ok := v.(string)
	if ok {
		return str
	}

	// Convert to string as widely accepting as you can.
	return fmt.Sprint(v)
}

func NewServer(red *redis.Client, opts *Options, handlers map[string]Handler) *Server {
	return NewServerWithAdapter(&RedisAdapter{red}, opts, handlers)
}

func NewServerWithAdapter(red DbAdapter, opts *Options, handlers map[string]Handler) *Server {
	srv := &Server{
		red:      red,
		handlers: handlers,
		opts:     OptsWithDefaults(opts),
		queues:   []string{},
		queueMap: map[string]string{},
	}
	for func_name, _ := range handlers {
		qname := callQueueName(srv.opts.Prefix, func_name)
		srv.queues = append(srv.queues, qname)
		srv.queueMap[qname] = func_name
	}
	return srv
}

func (s *Server) Run() {
	for !s.isClosing() {
		if !s.RunOnce() {
			return
		}
	}
}

func (s *Server) RunOnce() bool {
	res, err := s.red.BLPop(time.Second, rotated(s.queues, s.iterations)...)
	s.iterations += 1

	if err != nil && err != redis.Nil {
		log.Print("Error in BLPOP: ", err)
		return false
	}
	if len(res) > 1 {
		s.handleBLPopResult(res)
	}
	return true
}

func (s *Server) Close() {
	atomic.AddInt64(&s.closing, 1)
	// TODO: wait for it to actually close
}

func (s *Server) isClosing() bool {
	return atomic.LoadInt64(&s.closing) > 0
}

func (s *Server) handleBLPopResult(res []string) error {
	queue := res[0]
	msg := res[1]

	req := &RequestImpl{}
	err := json.Unmarshal([]byte(msg), req)
	if err != nil {
		log.Printf("Could not unmarshal request: %s", err)
		return err
	}

	func_name := s.queueMap[queue]
	handler := s.handlers[func_name]
	return s.callHandler(func_name, req, handler)
}

func (s *Server) callHandler(func_name string, req *RequestImpl, handler Handler) error {
	defer func() {
		recovered := recover()
		if recovered != nil {
			log.Print("ERR:", recovered)
			switch v := recovered.(type) {
			case error:
				log.Print("ERR:", v)
				s.sendResponse(func_name, req, ErrResponse{
					Ts:  s.timestamp(),
					Err: v.Error(),
				})
			case fmt.Stringer:
				s.sendResponse(func_name, req, ErrResponse{
					Ts:  s.timestamp(),
					Err: v.String(),
				})
			case string:
				s.sendResponse(func_name, req, ErrResponse{
					Ts:  s.timestamp(),
					Err: v,
				})
			default:
				s.sendResponse(func_name, req, ErrResponse{
					Ts:  s.timestamp(),
					Err: "other error",
				})
			}
		}
	}()

	res, err := handler.ServeRPC(req)
	if err != nil {
		s.sendResponse(func_name, req, ErrResponse{
			Ts:  s.timestamp(),
			Err: err.Error(),
		})
	} else {
		s.sendResponse(func_name, req, ResResponse{
			Ts:  s.timestamp(),
			Res: res,
		})
	}

	return nil
}

type ErrResponse struct {
	Ts  string `json:"ts"`
	Err string `json:"err"`
}

type ResResponse struct {
	Ts  string      `json:"ts"`
	Res interface{} `json:"res"`
}

func (s *Server) sendResponse(func_name string, req *RequestImpl, res interface{}) error {
	msg, err := json.Marshal(res)
	if err != nil {
		log.Println("Error while marshaling reply: ", res)
		return err
	}

	qn := responseQueueName(s.opts.Prefix, func_name, req.Id)
	if err = s.red.RPushEx(qn, string(msg), s.opts.ResultExpire); err != nil {
		log.Println("Error while sending reply: ", err)
		return err
	}

	return nil
}

func (s *Server) timestamp() string {
	return s.opts.TimeSource().Format(time.RFC3339)
}

func rotated(s []string, i int) []string {
	if len(s) == 0 {
		return s
	}
	i = i % len(s)
	return append(s[i:], s[:i]...)
}
