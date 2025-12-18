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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}

type Value struct {
	val    string
	expiry time.Time
}

func handleConn(conn net.Conn) {
	kv := make(map[string]Value)

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
			kv[sc.Key] = Value{val: sc.Value, expiry: sc.Expiry}
			OutputSimpleString("OK", conn)
		case "GET":
			if len(c.Args) < 1 {
				OutputNullSimpleString(conn)
			}
			val, ok := kv[c.Args[0]]
			if !ok {
				OutputNullSimpleString(conn)
			} else {
				if val.expiry.IsZero() || val.expiry.After(time.Now()) {
					OutputBulkStrings([]string{val.val}, conn)
				} else { // Expired
					delete(kv, c.Args[0])
					OutputNullSimpleString(conn)
				}
			}
		default:
			slog.Error("Unknown command", "name", c.Name, slog.Group("args", c.Args))
		}
	}
}
