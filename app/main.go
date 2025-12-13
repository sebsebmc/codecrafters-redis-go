package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
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

func handleConn(conn net.Conn) {
	messageQueue := make([][]byte, 0)

	br := bufio.NewReader(conn)

	for {
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		buf, err := br.ReadBytes('\n')

		if err != nil {
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				slog.Debug("Connection closing due to error", "error", err)
				return
			}
		}
		messageQueue = append(messageQueue, buf)
		if bytes.Contains(bytes.ToLower(buf), []byte("ping")) {
			conn.Write([]byte("+PONG\r\n"))
		}
	}
}
