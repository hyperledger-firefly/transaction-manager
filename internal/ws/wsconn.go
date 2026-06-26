// Copyright © 2022 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ws

import (
	"context"
	"reflect"
	"strings"
	"sync"

	ws "github.com/gorilla/websocket"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
)

type webSocketConnection struct {
	ctx       context.Context
	id        string
	server    *webSocketServer
	conn      *ws.Conn
	mux       sync.Mutex
	closed    bool
	topics    map[string]*webSocketTopic
	broadcast chan interface{}
	newTopic  chan bool
	closing   chan struct{}
}

type WebSocketCommandMessageOrError struct {
	Msg *WebSocketCommandMessage
	Err error
}

type WebSocketCommandMessage struct {
	Type        string `json:"type,omitempty"`
	Topic       string `json:"topic,omitempty"`  // synonym for "topic" - from a time when we let you configure the topic separate to the stream name
	Stream      string `json:"stream,omitempty"` // name of the event stream
	Message     string `json:"message,omitempty"`
	BatchNumber int64  `json:"batchNumber,omitempty"`
}

func newConnection(bgCtx context.Context, server *webSocketServer, conn *ws.Conn) *webSocketConnection {
	id := fftypes.NewUUID().String()
	wsc := &webSocketConnection{
		ctx:       log.WithLogFields(bgCtx, "wsc", id),
		id:        id,
		server:    server,
		conn:      conn,
		newTopic:  make(chan bool),
		topics:    make(map[string]*webSocketTopic),
		broadcast: make(chan interface{}),
		closing:   make(chan struct{}),
	}
	go wsc.listen()
	go wsc.sender()
	return wsc
}

func (c *webSocketConnection) close() {
	c.mux.Lock()
	if !c.closed {
		c.closed = true
		c.conn.Close()
		close(c.closing)
	}
	c.mux.Unlock()

	for _, t := range c.topics {
		c.server.cycleTopic(c.id, t)
		log.L(c.ctx).Infof("Websocket closed while active on topic '%s'", t.topic)
	}
	c.server.connectionClosed(c)
	log.L(c.ctx).Infof("Disconnected")
}

func (c *webSocketConnection) sender() {
	defer c.close()
	buildCases := func() []reflect.SelectCase {
		c.mux.Lock()
		defer c.mux.Unlock()
		cases := make([]reflect.SelectCase, len(c.topics)+3)
		i := 0
		for _, t := range c.topics {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.senderChannel)}
			i++
		}
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.broadcast)}
		i++
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.closing)}
		i++
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.newTopic)}
		return cases
	}
	cases := buildCases()
	for {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			log.L(c.ctx).Infof("Closing")
			return
		}

		if chosen == len(cases)-1 {
			// Addition of a new topic
			cases = buildCases()
		} else {
			// Message from one of the existing topics
			_ = c.conn.WriteJSON(value.Interface())
		}
	}
}

func (c *webSocketConnection) listenTopic(t *webSocketTopic) {
	c.mux.Lock()
	c.topics[t.topic] = t
	c.server.ListenOnTopic(c, t.topic)
	c.mux.Unlock()
	select {
	case c.newTopic <- true:
	case <-c.closing:
	}
}

func (c *webSocketConnection) listenReplies() {
	c.server.ListenForReplies(c)
}

func (c *webSocketConnection) listen() {
	defer c.close()
	log.L(c.ctx).Infof("Connected")
	for {
		var msg WebSocketCommandMessage
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			log.L(c.ctx).Errorf("Error: %s", err)
			return
		}
		log.L(c.ctx).Tracef("Received: %+v", msg)

		topic := msg.Stream
		if topic == "" {
			topic = msg.Topic
		}
		t := c.server.getTopic(topic)
		switch strings.ToLower(msg.Type) {
		case "listen":
			c.listenTopic(t)
		case "listenreplies":
			c.listenReplies()
		case "ack":
			if !c.dispatchAckOrError(t, &msg, nil) {
				return
			}
		case "error":
			if !c.dispatchAckOrError(t, &msg, i18n.NewError(c.ctx, tmmsgs.MsgWSErrorFromClient, msg.Message)) {
				return
			}
		default:
			log.L(c.ctx).Errorf("Unexpected message type: %+v", msg)
		}
	}
}

func (c *webSocketConnection) dispatchAckOrError(t *webSocketTopic, msg *WebSocketCommandMessage, err error) bool {
	if err != nil {
		log.L(c.ctx).Debugf("Received WebSocket error on topic '%s': %s", t.topic, err)
	} else {
		log.L(c.ctx).Debugf("Received WebSocket ack for batch %d on topic '%s'", msg.BatchNumber, t.topic)
	}
	select {
	case t.receiverChannel <- &WebSocketCommandMessageOrError{Msg: msg, Err: err}:
	default:
		log.L(c.ctx).Debugf("Received WebSocket ack for batch %d on topic '%s'. Too many spurious acks - closing connection", msg.BatchNumber, t.topic)
		// This shouldn't happen, as the channel has a buffer. So this means the client has sent us a number of
		// acks that are not on the right topic (so no event stream is attached).
		// We cannot discard this ack and continue, but we cannot afford to block here either, so we close the websocket
		return false
	}
	return true
}
