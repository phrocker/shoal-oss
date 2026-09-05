// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package mcp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// jsonRPCVersion is the only envelope version this transport accepts. A
// message carrying anything else is rejected rather than coerced, so a client
// speaking a different protocol fails loudly at the envelope instead of
// silently having its fields reinterpreted.
const jsonRPCVersion = "2.0"

// JSON-RPC 2.0 reserved error codes. Only these are used for protocol-level
// failures; a tool that runs and fails reports through ToolResult.IsError
// instead, so a caller can always distinguish "the call never happened" from
// "the call happened and returned an error". Conflating the two is the fourth
// weakness recorded against the prior-art MCP surface in issue #277.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

// maxMessageBytes bounds a single inbound message. stdio is a trusted-ish
// channel but not an unbounded one: without a cap, a malformed or hostile peer
// can drive this process out of memory with one unterminated line. The limit
// is deliberately generous relative to any real tool call.
const maxMessageBytes = 8 << 20

// Request is an inbound JSON-RPC 2.0 message. ID is kept as raw JSON because
// the specification permits a string, a number, or null, and a response must
// echo the client's spelling exactly rather than a re-encoded equivalent.
//
// A message with no ID is a notification: it expects no reply, and the server
// must stay silent even on failure.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// isNotification reports whether the message expects no response. Absent and
// literal-null IDs are both notifications under the specification.
func (r Request) isNotification() bool {
	if len(r.ID) == 0 {
		return true
	}
	return string(r.ID) == "null"
}

// Response is an outbound JSON-RPC 2.0 message. Exactly one of Result or Error
// is populated; newResponse and newErrorResponse are the only constructors so
// that invariant cannot be violated by field assignment at a call site.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error implements error so a protocol failure can travel as an ordinary Go
// error until the transport serializes it.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("jsonrpc error %d: %s", e.Code, e.Message)
}

// newError builds a protocol-level error carrying no operational detail. The
// message is a fixed string chosen by this package; upstream error text is
// deliberately not interpolated, because a protocol error is answered before
// authorization has necessarily been established and must not become a channel
// for describing corpus state to an unauthenticated peer.
func newError(code int, message string) *Error {
	return &Error{Code: code, Message: message}
}

// newResponse builds a success response for the given client ID.
func newResponse(id json.RawMessage, result any) Response {
	encoded, err := json.Marshal(result)
	if err != nil {
		// A result this package constructed must always marshal. If it does
		// not, report an internal error rather than emitting a response whose
		// Result field is absent, which a client would read as a malformed
		// success.
		return newErrorResponse(id, newError(
			codeInternalError, "result could not be encoded"))
	}
	return Response{JSONRPC: jsonRPCVersion, ID: id, Result: encoded}
}

// newErrorResponse builds a failure response for the given client ID.
func newErrorResponse(id json.RawMessage, failure *Error) Response {
	if failure == nil {
		failure = newError(codeInternalError, "unspecified error")
	}
	return Response{JSONRPC: jsonRPCVersion, ID: id, Error: failure}
}

// codec frames JSON-RPC messages as newline-delimited JSON, which is the MCP
// stdio transport framing. Note this is deliberately not the Content-Length
// header framing used by the Language Server Protocol; the two are frequently
// confused and are not interchangeable.
//
// Encoding is serialized by the caller (Server.Serve reads and writes from a
// single goroutine), so the codec itself holds no lock.
type codec struct {
	reader *bufio.Reader
	writer *bufio.Writer
}

func newCodec(input io.Reader, output io.Writer) *codec {
	return &codec{
		reader: bufio.NewReaderSize(input, 64<<10),
		writer: bufio.NewWriter(output),
	}
}

// errMessageTooLarge is returned when a single line exceeds maxMessageBytes.
var errMessageTooLarge = errors.New("mcp: message exceeds maximum size")

// readMessage returns the next raw message. It skips blank lines, which some
// clients emit as keepalives, and returns io.EOF when the peer closes.
//
// bufio.Reader.ReadString is used rather than bufio.Scanner because a Scanner
// silently truncates at its buffer limit, turning an oversized message into a
// corrupt but well-formed-looking one. Here an oversized message is an
// explicit error instead.
func (c *codec) readMessage() ([]byte, error) {
	for {
		var line []byte
		for {
			chunk, err := c.reader.ReadSlice('\n')
			if len(line)+len(chunk) > maxMessageBytes {
				return nil, errMessageTooLarge
			}
			line = append(line, chunk...)
			if err == nil {
				break
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			if errors.Is(err, io.EOF) && len(line) > 0 {
				break
			}
			return nil, err
		}
		trimmed := trimSpace(line)
		if len(trimmed) == 0 {
			continue
		}
		return trimmed, nil
	}
}

// writeMessage encodes one response and flushes it. Flushing per message is
// required: a client blocks awaiting the reply, so a buffered response that is
// never flushed deadlocks the session.
func (c *codec) writeMessage(message any) error {
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if _, err := c.writer.Write(encoded); err != nil {
		return err
	}
	if err := c.writer.WriteByte('\n'); err != nil {
		return err
	}
	return c.writer.Flush()
}

// trimSpace removes leading and trailing ASCII whitespace without allocating.
func trimSpace(value []byte) []byte {
	start := 0
	for start < len(value) && isSpace(value[start]) {
		start++
	}
	end := len(value)
	for end > start && isSpace(value[end-1]) {
		end--
	}
	return value[start:end]
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// decodeRequest parses and validates one inbound envelope. Validation is
// strict: a message that does not declare JSON-RPC 2.0, or that carries no
// method, is rejected rather than processed on a guess.
func decodeRequest(raw []byte) (Request, *Error) {
	var request Request
	if err := json.Unmarshal(raw, &request); err != nil {
		return Request{}, newError(codeParseError, "invalid JSON")
	}
	if request.JSONRPC != jsonRPCVersion {
		return Request{}, newError(
			codeInvalidRequest, "unsupported jsonrpc version")
	}
	if request.Method == "" {
		return Request{}, newError(codeInvalidRequest, "method is required")
	}
	return request, nil
}
