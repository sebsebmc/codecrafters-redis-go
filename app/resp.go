package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"
)

type Command struct {
	Name string
	Args []string
}

type RespParser struct {
	reader *bufio.Reader
}

func MakeRespParser(in io.Reader) *RespParser {
	return &RespParser{
		reader: bufio.NewReader(in),
	}
}

func (r *RespParser) Parse() (*Command, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to parse command reading: %w", err)
	}
	if b != '*' {
		return nil, fmt.Errorf("command is not an array, expected '*' got '%c'", b)
	}

	c := new(Command)

	arrLen, err := r.parseSize()
	if err != nil {
		return nil, err
	}

	for i := 0; i < arrLen; i++ {
		str, err := r.parseBulkString()
		if err != nil {
			return nil, err
		}
		if i == 0 {
			c.Name = str
		} else {
			c.Args = append(c.Args, str)
		}
	}

	return c, nil

}

func (r *RespParser) parseSize() (int, error) {
	bs, err := r.reader.ReadBytes('\r')
	if err != nil {
		return 0, fmt.Errorf("Did not find size in buffer: %w", err)
	}
	slog.Debug("Size bytes", "val", bs[:len(bs)-1])
	arrLen, err := strconv.ParseInt(string(bs[:len(bs)-1]), 10, 0)
	if err != nil {
		return 0, err
	}
	r.reader.Discard(1)
	return int(arrLen), nil
}

func (r *RespParser) parseBulkString() (string, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return "", fmt.Errorf("cannot read bulk string: %w", err)
	}

	if b != '$' {
		return "", fmt.Errorf("Invalid identifier for bulk string, expected '$' got '%c'", b)
	}

	strLen, err := r.parseSize()
	if err != nil {
		return "", err
	}
	bulkStr := make([]byte, strLen)
	r.reader.Read(bulkStr)
	slog.Debug("Read bulk string", "val", string(bulkStr))
	r.reader.Discard(2)
	return string(bulkStr), nil
}

func OutputBulkStrings(strs []string, wr io.Writer) {
	wr.Write([]byte("$"))
	for _, v := range strs {
		wr.Write([]byte(fmt.Sprintf("%d\r\n%s\r\n", len(v), v)))
	}
}

type SetCommand struct {
	Key    string
	Value  string
	Expiry time.Time
}

func ValidateSetCommand(c *Command) (*SetCommand, error) {
	if c.Name != "SET" {
		return nil, fmt.Errorf("command name is not SET")
	}
	if !(len(c.Args) == 2 || len(c.Args) == 4) {
		return nil, fmt.Errorf("command SET has invalid number of arguments: %d", len(c.Args))
	}
	sc := new(SetCommand)
	sc.Key = c.Args[0]
	sc.Value = c.Args[1]

	if len(c.Args) == 4 {
		num, err := strconv.Atoi(c.Args[3])
		if err != nil {
			return nil, err
		}
		switch c.Args[2] {
		case "EX":
			sc.Expiry = time.Now().Add(time.Duration(num) * time.Second)
		case "PX":
			sc.Expiry = time.Now().Add(time.Duration(num) * time.Millisecond)
		default:
			return nil, fmt.Errorf("invalid expiry specifier for SET: '%s'", c.Args[2])
		}
	}
	return sc, nil
}

func OutputSimpleString(str string, wr io.Writer) {
	wr.Write(fmt.Appendf(nil, "+%s\r\n", str))
}

func OutputNullSimpleString(wr io.Writer) {
	wr.Write([]byte("$-1\r\n"))
}

func OutputInteger(i int, wr io.Writer) {
	wr.Write(fmt.Appendf(nil, ":%d\r\n", i))
}

func OutputArray(strs []string, wr io.Writer) {
	wr.Write(fmt.Appendf(nil, "*%d\r\n", len(strs)))
	for i := 0; i < len(strs); i++ {
		OutputBulkStrings([]string{strs[i]}, wr)
	}
}
