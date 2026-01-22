package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	slog.SetLogLoggerLevel(slog.LevelDebug)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	s := MakeServer()
	ctx := context.Background()

	go s.run(ctx)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go s.handleConn(ctx, conn)
	}
}

type Value struct {
	val    string
	expiry time.Time
}

type Job struct {
	client net.Conn
	work   func()
}

type WaitingClient struct {
	client net.Conn
	t      *time.Timer
}

type Server struct {
	simpleMap map[string]Value
	lists     map[string][]string
	waiting   map[string][]WaitingClient
	jobs      chan func()
}

func MakeServer() *Server {
	s := new(Server)
	s.simpleMap = make(map[string]Value)
	s.lists = make(map[string][]string)
	s.waiting = make(map[string][]WaitingClient)
	s.jobs = make(chan func(), 128)
	return s
}

func (s *Server) run(ctx context.Context) {
	for {
		select {
		case j := <-s.jobs:
			j()
		case <-ctx.Done():
		}
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	for {
		rp := MakeRespParser(conn)
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		c, err := rp.Parse()

		if err != nil {
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				slog.Debug("Connection error", "message", err)
				return
			}
			continue
		}
		slog.Info("Command received", "name", c.Name)

		switch c.Name {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			s.jobs <- func() {
				OutputBulkStrings(c.Args, conn)
			}
		case "SET":
			s.jobs <- func() {
				sc, err := ValidateSetCommand(c)
				if err != nil {
					slog.Error(err.Error())
					return
				}
				s.simpleMap[sc.Key] = Value{val: sc.Value, expiry: sc.Expiry}
				OutputSimpleString("OK", conn)
			}
		case "GET":
			s.jobs <- func() {
				if len(c.Args) < 1 {
					OutputNullSimpleString(conn)
					return
				}
				val, ok := s.simpleMap[c.Args[0]]
				if !ok {
					OutputNullSimpleString(conn)
				} else {
					if val.expiry.IsZero() || val.expiry.After(time.Now()) {
						OutputBulkStrings([]string{val.val}, conn)
					} else { // Expired
						delete(s.simpleMap, c.Args[0])
						OutputNullSimpleString(conn)
					}
				}
			}
		case "RPUSH":
			s.jobs <- func() {
				rpc, err := ValidateRPushCommand(c)
				if err != nil {
					slog.Error(err.Error())
					return
				}
				s.lists[rpc.ListKey] = append(s.lists[rpc.ListKey], rpc.Values...)
				OutputInteger(len(s.lists[rpc.ListKey]), conn)
				if len(s.waiting[rpc.ListKey]) > 0 {
					waiting := s.waiting[rpc.ListKey][0]
					first := s.lists[rpc.ListKey][0]
					s.lists[rpc.ListKey] = s.lists[rpc.ListKey][1:]
					OutputArray([]string{rpc.ListKey, first}, waiting.client)
				}
			}
		case "LRANGE":
			s.jobs <- func() {
				lrc, err := ValidateLRangeCommand(c)
				if err != nil {
					slog.Error(err.Error())
					return
				}
				start := lrc.Start
				end := lrc.End
				listLen := len(s.lists[lrc.ListKey])
				if lrc.Start < 0 {
					start = max(0, lrc.Start+listLen)
				} else {
					start = min(lrc.Start, listLen)
				}
				if lrc.End < 0 {
					end = max(0, min(lrc.End+listLen+1, listLen))
				} else {
					end = min(listLen, end+1)
				}
				slog.Debug("'LRANGE' command from", "start", start, "end", end)
				OutputArray(s.lists[lrc.ListKey][start:end], conn)
			}
		case "LPUSH":
			s.jobs <- func() {
				lpc, err := ValidateLPushCommand(c)
				if err != nil {
					slog.Error(err.Error())
					return
				}
				reversed := make([]string, len(lpc.Values))
				for i := 0; i < len(lpc.Values); i++ {
					reversed[i] = lpc.Values[len(lpc.Values)-(i+1)]
				}
				s.lists[lpc.ListKey] = append(reversed, s.lists[lpc.ListKey]...)
				OutputInteger(len(s.lists[lpc.ListKey]), conn)
				if len(s.waiting[lpc.ListKey]) > 0 {
					waiting := s.waiting[lpc.ListKey][0]
					first := s.lists[lpc.ListKey][0]
					s.lists[lpc.ListKey] = s.lists[lpc.ListKey][1:]
					OutputArray([]string{lpc.ListKey, first}, waiting.client)
				}
			}
		case "LLEN":
			s.jobs <- func() {
				list, ok := s.lists[c.Args[0]]
				if !ok {
					OutputInteger(0, conn)
					return
				}
				OutputInteger(len(list), conn)
			}
		case "LPOP":
			s.jobs <- func() {
				lpc, err := ValidateLPopCommand(c)
				if err != nil {
					slog.Error(err.Error())
					return
				}
				list, ok := s.lists[lpc.ListKey]
				if !ok || len(list) == 0 {
					OutputNullSimpleString(conn)
					return
				}
				idx := min(len(list), lpc.Count)
				val := list[:idx]
				s.lists[lpc.ListKey] = list[idx:]
				if lpc.Count > 1 {
					OutputArray(val, conn)
				} else {
					OutputBulkStrings(val, conn)
				}
			}
		case "BLPOP":
			s.jobs <- func() {
				blpc, err := ValidateBLPopCommand(c)
				if err != nil {
					slog.Error(err.Error())
					return
				}
				// As long as we always make sure to fulfill blocking clients when receiving items this is fine
				if len(s.lists[blpc.ListKey]) > 0 {
					OutputArray(s.lists[blpc.ListKey][:1], conn)
					return
				}
				if blpc.Timeout == 0 {
					s.waiting[blpc.ListKey] = append(s.waiting[blpc.ListKey], WaitingClient{conn, nil})
				} else {
					timer := time.AfterFunc(time.Duration(blpc.Timeout), func() {
						s.jobs <- func() {
							for i, v := range s.waiting[blpc.ListKey] {
								// We could make waiting a map of maps as well to make this not O(n) on the number of waiting clients
								if v.client == conn { // Should only be one because its a "blocking" wait for the client
									s.waiting[blpc.ListKey] = slices.Delete(s.waiting[blpc.ListKey], i, i+1)
									OutputNullArray(v.client)
									break
								}
							}
						}
					})
					s.waiting[blpc.ListKey] = append(s.waiting[blpc.ListKey], WaitingClient{conn, timer})
				}
			}
		default:
			slog.Error("Unknown command", "name", c.Name, slog.Group("args", c.Args))
		}
	}
}
