package live

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tomasz-tomczyk/crit/internal/server"
)

func TestCDPPageControllerStartSendReloadAndEvents(t *testing.T) {
	var upgrader websocket.Upgrader
	var mu sync.Mutex
	var methods []string
	var newDocumentSource string
	commands := make(chan map[string]any, 32)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var command map[string]any
			if conn.ReadJSON(&command) != nil {
				return
			}
			method, _ := command["method"].(string)
			mu.Lock()
			methods = append(methods, method)
			if method == "Page.addScriptToEvaluateOnNewDocument" {
				params := command["params"].(map[string]any)
				newDocumentSource, _ = params["source"].(string)
			}
			mu.Unlock()
			commands <- command
			if err := conn.WriteJSON(map[string]any{"id": command["id"], "result": map[string]any{}}); err != nil {
				return
			}
			if method == "Runtime.enable" {
				_ = conn.WriteJSON(map[string]any{
					"method": "Runtime.executionContextCreated",
					"params": map[string]any{"context": map[string]any{"id": 17, "auxData": map[string]any{"isDefault": true}}},
				})
			}
			if method == "Page.enable" {
				_ = conn.WriteJSON(map[string]any{
					"method": "Runtime.bindingCalled",
					"params": map[string]any{"name": cdpBindingName, "payload": `{"type":"agent-ready"}`},
				})
			}
			if method == "Page.reload" {
				_ = conn.WriteJSON(map[string]any{"method": "Page.loadEventFired", "params": map[string]any{}})
			}
			if method == "Runtime.evaluate" {
				params, _ := command["params"].(map[string]any)
				expression, _ := params["expression"].(string)
				if strings.HasPrefix(expression, "window.__critCDPReceive") && strings.Contains(expression, CDPAgentHandshakeType) {
					_ = conn.WriteJSON(map[string]any{
						"method": "Runtime.bindingCalled",
						"params": map[string]any{"name": cdpBindingName, "payload": `{"type":"agent-ready"}`},
					})
				}
			}
		}
	}))
	defer srv.Close()

	controller, err := NewCDPPageController("ws" + strings.TrimPrefix(srv.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	messages := make(chan json.RawMessage, 1)
	controller.SetMessageCallback(func(message json.RawMessage) { messages <- message })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := controller.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer controller.Close()

	select {
	case got := <-messages:
		if string(got) != `{"type":"agent-ready"}` {
			t.Fatalf("callback message = %s", got)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for binding message")
	}

	if err := controller.Send(ctx, json.RawMessage(`{"type":"set-mode","value":"pin"}`)); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reload(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	gotMethods := append([]string(nil), methods...)
	gotSource := newDocumentSource
	mu.Unlock()
	for _, want := range []string{"Runtime.enable", "Page.enable", "Runtime.addBinding", "Page.addScriptToEvaluateOnNewDocument", "Runtime.evaluate", "Page.reload"} {
		if !containsString(gotMethods, want) {
			t.Errorf("missing CDP method %s in %v", want, gotMethods)
		}
	}
	if !strings.Contains(gotSource, "data:text/javascript,/crit-agent.js") || !strings.Contains(gotSource, "data-crit-marker-css") {
		t.Fatalf("new-document source lacks bootstrap or CSS injection")
	}
	lastIndex := -1
	for _, name := range server.AgentScriptFiles {
		index := strings.Index(gotSource, "sourceURL=crit-cdp/"+name)
		if index <= lastIndex {
			t.Fatalf("agent script %s missing or out of order", name)
		}
		lastIndex = index
	}

	var sawSend bool
	for !sawSend {
		select {
		case command := <-commands:
			if command["method"] != "Runtime.evaluate" {
				continue
			}
			params := command["params"].(map[string]any)
			expression, _ := params["expression"].(string)
			if strings.Contains(expression, "window.__critCDPReceive") && strings.Contains(expression, "set-mode") {
				sawSend = true
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for command evaluation")
		}
	}
}

func TestCDPPageControllerRejectsInvalidJSONAndIgnoresInvalidBinding(t *testing.T) {
	controller, err := NewCDPPageController("ws://unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Send(context.Background(), json.RawMessage(`{`)); err == nil || !strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("invalid JSON error = %v", err)
	}

	controller.handleEvent(cdpPageEnvelope{
		Method: "Runtime.bindingCalled",
		Params: json.RawMessage(`{"name":"__critCDPSend","payload":"not-json"}`),
	})
	select {
	case message := <-controller.messages:
		t.Fatalf("unexpected invalid message: %s", message)
	default:
	}
}

func TestCDPPageControllerSerializesAgentCommands(t *testing.T) {
	var upgrader websocket.Upgrader
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var command map[string]any
			if conn.ReadJSON(&command) != nil {
				return
			}
			params, _ := command["params"].(map[string]any)
			expression, _ := params["expression"].(string)
			if command["method"] == "Runtime.evaluate" && strings.HasPrefix(expression, "window.__critCDPReceive(JSON.parse(") {
				_ = conn.WriteJSON(map[string]any{"id": command["id"], "result": map[string]any{}})
				continue
			}
			if conn.WriteJSON(map[string]any{"id": command["id"], "result": map[string]any{}}) != nil {
				return
			}
		}
	}))
	defer srv.Close()

	controller, err := NewCDPPageController("ws" + strings.TrimPrefix(srv.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := controller.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer controller.Close()

	errs := make(chan error, 2)
	for _, command := range []json.RawMessage{
		json.RawMessage(`{"type":"clear-highlight","sequence":1}`),
		json.RawMessage(`{"type":"clear-highlight","sequence":2}`),
	} {
		go func(command json.RawMessage) { errs <- controller.Send(ctx, command) }(command)
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
