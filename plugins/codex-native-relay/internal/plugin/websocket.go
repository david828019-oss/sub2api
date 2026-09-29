package plugin

import (
	"context"
	"errors"
	"io"

	coderws "github.com/coder/websocket"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/relay"
)

// ForwardWebSocket 通过 codex relay 打开 Codex Responses WebSocket，并在宿主流与 relay
// 连接之间双向转发消息帧。首帧必须是 open；随后回 opened / rejected / error 之一。
func (s *Server) ForwardWebSocket(stream pluginv1.TransportPlugin_ForwardWebSocketServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open == nil {
		return sendWebSocketError(stream, &relay.Error{Code: "CODEX_RELAY_PROTOCOL", Message: "首帧必须是 open"})
	}
	opened, err := s.transport.DialWebSocket(ctx, relay.WebSocketRequest{
		URL:      open.GetUrl(),
		Headers:  headersFromPlugin(open.GetHeaders()),
		ProxyURL: open.GetProxyUrl(),
	})
	if err != nil {
		var rejected *relay.Rejected
		if errors.As(err, &rejected) {
			return stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Rejected{Rejected: &pluginv1.WebSocketRejected{
				StatusCode: int32(rejected.StatusCode),
				Headers:    headersToPlugin(rejected.Headers),
				Body:       rejected.Body,
			}}})
		}
		var relayErr *relay.Error
		if !errors.As(err, &relayErr) {
			relayErr = &relay.Error{Code: "CODEX_RELAY_INTERNAL", Message: "内部错误", RequestSent: true}
		}
		return sendWebSocketError(stream, relayErr)
	}
	conn := opened.Conn
	defer func() { _ = conn.CloseNow() }()
	if err := stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Opened{Opened: &pluginv1.WebSocketOpened{
		StatusCode: 101,
		Headers:    headersToPlugin(opened.Headers),
	}}}); err != nil {
		return err
	}

	received := make(chan struct{})
	sent := make(chan struct{})
	// 宿主 -> relay。
	go func() {
		defer close(received)
		for {
			frame, err := stream.Recv()
			if err != nil {
				_ = conn.Close(coderws.StatusGoingAway, "")
				return
			}
			switch payload := frame.GetFrame().(type) {
			case *pluginv1.WebSocketClientFrame_Message:
				messageType := coderws.MessageText
				if payload.Message.GetBinary() {
					messageType = coderws.MessageBinary
				}
				if err := conn.Write(ctx, messageType, payload.Message.GetPayload()); err != nil {
					return
				}
			case *pluginv1.WebSocketClientFrame_Close:
				_ = conn.Close(coderws.StatusCode(payload.Close.GetCode()), payload.Close.GetReason())
				return
			}
		}
	}()
	// relay -> 宿主。stream.Send 只在这个协程里调用，处理函数返回前必须等它结束。
	go func() {
		defer close(sent)
		for {
			messageType, payload, err := conn.Read(ctx)
			if err != nil {
				if code := coderws.CloseStatus(err); code != -1 {
					var closeErr coderws.CloseError
					reason := ""
					if errors.As(err, &closeErr) {
						reason = closeErr.Reason
					}
					_ = stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Close{Close: &pluginv1.WebSocketClose{
						Code:   int32(code),
						Reason: reason,
					}}})
				} else if ctx.Err() == nil && !errors.Is(err, io.EOF) {
					_ = sendWebSocketError(stream, &relay.Error{Code: "CODEX_RELAY_WEBSOCKET_READ", Message: "读取 relay WebSocket 失败", RequestSent: true})
				}
				return
			}
			if err := stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Message{Message: &pluginv1.WebSocketMessage{
				Binary:  messageType == coderws.MessageBinary,
				Payload: payload,
			}}}); err != nil {
				return
			}
		}
	}()
	select {
	case <-received:
	case <-sent:
	}
	cancel()
	_ = conn.CloseNow()
	<-sent
	return nil
}

func sendWebSocketError(stream pluginv1.TransportPlugin_ForwardWebSocketServer, err *relay.Error) error {
	return stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Error{Error: &pluginv1.ForwardResponseError{
		Code:        err.Code,
		Message:     err.Message,
		RequestSent: err.RequestSent,
	}}})
}
