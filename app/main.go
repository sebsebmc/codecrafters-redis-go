package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go s.handleConn(conn)
	}
}

type Value struct {
	val    string
	expiry time.Time
}

type Server struct {
	simpleMap map[string]Value
	lists     map[string][]string
}

func MakeServer() *Server {
	s := new(Server)
	s.simpleMap = make(map[string]Value)
	s.lists = make(map[string][]string)
	return s
}

func (s *Server) handleConn(conn net.Conn) {
	for {
		rp := MakeRespParser(conn)
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		c, err := rp.Parse()

		if err != nil {
			slog.Debug("Connection error", "message", err)
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				return
			}
		}
		slog.Info("Command received", "name", c.Name)

		switch strings.ToUpper(c.Name) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			OutputBulkStrings(c.Args, conn)
		case "SET":
			sc, err := ValidateSetCommand(c)
			if err != nil {
				slog.Error(err.Error())
			}
			s.simpleMap[sc.Key] = Value{val: sc.Value, expiry: sc.Expiry}
			OutputSimpleString("OK", conn)
		case "GET":
			if len(c.Args) < 1 {
				OutputNullSimpleString(conn)
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
		case "RPUSH":
			s.lists[c.Args[0]] = append(s.lists[c.Args[0]], c.Args[1:]...)
			OutputInteger(len(s.lists[c.Args[0]]), conn)
		default:
			slog.Error("Unknown command", "name", c.Name, slog.Group("args", c.Args))
		}
	}
}
