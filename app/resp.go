package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Command struct {
	Name string
	Args []string
}

type RespParser struct {
	pos    int
	reader *bufio.Reader
}

func MakeRespParser(in io.Reader) *RespParser {
	return &RespParser{
		pos:    0,
		reader: bufio.NewReader(in),
	}
}

func (r *RespParser) Parse() (*Command, error) {
	b, err := r.reader.ReadByte()
	if err != nil || b != '*' {
		return nil, fmt.Errorf("Command is not an array")
	}
	r.pos = 1

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
		return 0, fmt.Errorf("Did not find size in buffer")
	}
	arrLen, err := strconv.ParseInt(string(bs[:len(bs)-2]), 10, 0)
	if err != nil {
		return 0, err
	}
	r.reader.Discard(1)
	return int(arrLen), nil
}

func (r *RespParser) parseBulkString() (string, error) {
	b, err := r.reader.ReadByte()
	if err != nil || b != '$' {
		return "", fmt.Errorf("Invalid identifier for bulk string")
	}
	r.pos += 1
	strLen, err := r.parseSize()
	if err != nil {
		return "", err
	}
	bulkStr := make([]byte, strLen)
	r.reader.Read(bulkStr)
	return string(bulkStr), nil
}
