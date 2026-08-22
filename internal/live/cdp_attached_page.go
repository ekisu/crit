package live

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tomasz-tomczyk/crit/internal/server"
)

const (
	cdpBindingName        = "__critCDPSend"
	CDPAgentHandshakeType = "crit-cdp-handshake"
)

// CDPPageController controls a page target by connecting directly to its
// webSocketDebuggerUrl. A controller may be started once.
type CDPPageController struct {
	wsURL     string
	bootstrap string
	bundle    string

	mu                 sync.Mutex
	conn               *websocket.Conn
	nextID             int
	pending            map[int]chan cdpPageResponse
	started            bool
	closed             bool
	callback           func(json.RawMessage)
	disconnectCallback func()

	writeMu     sync.Mutex
	operationMu sync.Mutex
	closeOnce   sync.Once
	done        chan struct{}
	messages    chan json.RawMessage
	agentReady  chan struct{}
	pageLoaded  chan struct{}
}

type cdpPageResponse struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type cdpPageEnvelope struct {
	ID     int             `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// NewCDPPageController prepares a controller for a page-target WebSocket URL.
func NewCDPPageController(wsURL string) (*CDPPageController, error) {
	bootstrap, bundle, err := buildCDPAgentBundle()
	if err != nil {
		return nil, err
	}
	return &CDPPageController{
		wsURL:      wsURL,
		bootstrap:  bootstrap,
		bundle:     bundle,
		pending:    make(map[int]chan cdpPageResponse),
		done:       make(chan struct{}),
		messages:   make(chan json.RawMessage, 64),
		agentReady: make(chan struct{}, 1),
		pageLoaded: make(chan struct{}, 1),
	}, nil
}

// Start connects to the page target and installs Crit's agent in the current
// document and all documents subsequently created by the page.
func (c *CDPPageController) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("CDP page controller already started")
	}
	c.started = true
	c.mu.Unlock()

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.DialContext(ctx, c.wsURL, nil)
	if err != nil {
		return fmt.Errorf("opening page DevTools WebSocket: %w", err)
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	go c.readLoop()
	go c.callbackLoop()

	for _, call := range []struct {
		method string
		params map[string]any
	}{
		{method: "Runtime.enable"},
		{method: "Page.enable"},
		{method: "Runtime.addBinding", params: map[string]any{"name": cdpBindingName}},
		{method: "Page.addScriptToEvaluateOnNewDocument", params: map[string]any{"source": c.bundle}},
		{method: "Runtime.evaluate", params: runtimeEvaluateParams(c.bundle, 0)},
	} {
		if _, err := c.call(ctx, call.method, call.params); err != nil {
			_ = c.Close()
			return err
		}
	}
	return nil
}

// SetMessageCallback replaces the callback for valid JSON messages emitted by
// the injected agent. Passing nil disables delivery.
func (c *CDPPageController) SetMessageCallback(callback func(json.RawMessage)) {
	c.mu.Lock()
	c.callback = callback
	c.mu.Unlock()
}

// SetDisconnectCallback installs a callback invoked when the DevTools
// connection ends. It is intended to terminate the owning live daemon so a
// subsequent CLI invocation can discover and attach to a replacement target.
func (c *CDPPageController) SetDisconnectCallback(callback func()) {
	c.mu.Lock()
	c.disconnectCallback = callback
	c.mu.Unlock()
}

// Send delivers a JSON agent command as a native same-origin message event.
func (c *CDPPageController) Send(ctx context.Context, command json.RawMessage) error {
	if !json.Valid(command) {
		return errors.New("agent command is not valid JSON")
	}
	encoded, _ := json.Marshal(string(command))
	expression := "window.__critCDPReceive(JSON.parse(" + string(encoded) + "))"
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	_, err := c.call(ctx, "Runtime.evaluate", runtimeEvaluateParams(expression, 0))
	return err
}

// RequestAgentReady asks the bridge to emit a fresh agent-ready message.
func (c *CDPPageController) RequestAgentReady(ctx context.Context) error {
	return c.Send(ctx, json.RawMessage(`{"type":"`+CDPAgentHandshakeType+`"}`))
}

// Reload reloads the attached page. The new document is instrumented by the
// Page.addScriptToEvaluateOnNewDocument registration installed by Start.
func (c *CDPPageController) Reload(ctx context.Context) error {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	drainSignal(c.pageLoaded)
	if _, err := c.call(ctx, "Page.reload", nil); err != nil {
		return err
	}
	select {
	case <-c.pageLoaded:
	case <-ctx.Done():
		return fmt.Errorf("waiting for reloaded page: %w", ctx.Err())
	case <-c.done:
		return errors.New("waiting for reloaded page: DevTools connection closed")
	}
	drainSignal(c.agentReady)
	encoded, _ := json.Marshal(`{"type":"` + CDPAgentHandshakeType + `"}`)
	expression := "window.__critCDPReceive(JSON.parse(" + string(encoded) + "))"
	if _, err := c.call(ctx, "Runtime.evaluate", runtimeEvaluateParams(expression, 0)); err != nil {
		return err
	}
	select {
	case <-c.agentReady:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for reloaded agent: %w", ctx.Err())
	case <-c.done:
		return errors.New("waiting for reloaded agent: DevTools connection closed")
	}
}

func drainSignal(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
}

// Close stops the controller and unblocks outstanding commands.
func (c *CDPPageController) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		conn := c.conn
		c.mu.Unlock()
		if conn != nil {
			c.writeMu.Lock()
			closeErr = conn.Close()
			c.writeMu.Unlock()
		} else {
			c.finish(errors.New("CDP page controller closed"))
		}
	})
	return closeErr
}

func (c *CDPPageController) call(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	c.mu.Lock()
	if c.conn == nil || c.closed {
		c.mu.Unlock()
		return nil, errors.New("CDP page controller is not running")
	}
	c.nextID++
	id := c.nextID
	response := make(chan cdpPageResponse, 1)
	c.pending[id] = response
	conn := c.conn
	c.mu.Unlock()

	payload, err := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
	if err == nil {
		c.writeMu.Lock()
		err = conn.WriteMessage(websocket.TextMessage, payload)
		c.writeMu.Unlock()
	}
	if err != nil {
		c.removePending(id)
		return nil, fmt.Errorf("sending %s: %w", method, err)
	}

	select {
	case reply, ok := <-response:
		if !ok {
			return nil, fmt.Errorf("%s: DevTools connection closed", method)
		}
		if reply.Error != nil {
			return nil, fmt.Errorf("%s: %s", method, reply.Error.Message)
		}
		if method == "Runtime.evaluate" {
			var evaluated struct {
				ExceptionDetails *struct {
					Text      string `json:"text"`
					Exception struct {
						Description string `json:"description"`
					} `json:"exception"`
				} `json:"exceptionDetails"`
			}
			if json.Unmarshal(reply.Result, &evaluated) == nil && evaluated.ExceptionDetails != nil {
				detail := evaluated.ExceptionDetails.Exception.Description
				if detail == "" {
					detail = evaluated.ExceptionDetails.Text
				}
				return nil, fmt.Errorf("%s: %s", method, detail)
			}
		}
		return reply.Result, nil
	case <-ctx.Done():
		c.removePending(id)
		return nil, fmt.Errorf("%s: %w", method, ctx.Err())
	case <-c.done:
		return nil, fmt.Errorf("%s: DevTools connection closed", method)
	}
}

func (c *CDPPageController) removePending(id int) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *CDPPageController) readLoop() {
	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			c.finish(err)
			return
		}
		var envelope cdpPageEnvelope
		if json.Unmarshal(payload, &envelope) != nil {
			continue
		}
		if envelope.ID != 0 {
			var response cdpPageResponse
			if json.Unmarshal(payload, &response) == nil {
				c.mu.Lock()
				waiter := c.pending[response.ID]
				delete(c.pending, response.ID)
				c.mu.Unlock()
				if waiter != nil {
					waiter <- response
				}
			}
			continue
		}
		c.handleEvent(envelope)
	}
}

func (c *CDPPageController) handleEvent(event cdpPageEnvelope) {
	switch event.Method {
	case "Runtime.bindingCalled":
		var params struct {
			Name    string          `json:"name"`
			Payload json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(event.Params, &params) != nil || params.Name != cdpBindingName {
			return
		}
		var raw string
		if json.Unmarshal(params.Payload, &raw) != nil || !json.Valid([]byte(raw)) {
			return
		}
		var message struct {
			Type string `json:"type"`
		}
		if json.Unmarshal([]byte(raw), &message) == nil && message.Type == "agent-ready" {
			select {
			case c.agentReady <- struct{}{}:
			default:
			}
		}
		select {
		case c.messages <- json.RawMessage(raw):
		case <-c.done:
		}
	case "Page.loadEventFired":
		select {
		case c.pageLoaded <- struct{}{}:
		default:
		}
	}
}

func (c *CDPPageController) callbackLoop() {
	for {
		select {
		case message := <-c.messages:
			c.mu.Lock()
			callback := c.callback
			c.mu.Unlock()
			if callback != nil {
				callback(message)
			}
		case <-c.done:
			return
		}
	}
}

func (c *CDPPageController) finish(_ error) {
	c.closeOnce.Do(func() {})
	c.mu.Lock()
	c.closed = true
	disconnectCallback := c.disconnectCallback
	for id, waiter := range c.pending {
		delete(c.pending, id)
		close(waiter)
	}
	c.mu.Unlock()
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	if disconnectCallback != nil {
		disconnectCallback()
	}
}

func runtimeEvaluateParams(expression string, contextID int) map[string]any {
	params := map[string]any{"expression": expression, "awaitPromise": false}
	if contextID != 0 {
		params["contextId"] = contextID
	}
	return params
}

func buildCDPAgentBundle() (string, string, error) {
	bootstrap := `(function(){
if (window.__critCDPBridgeInstalled) return;
window.__critCDPBridgeInstalled = true;
var nativePostMessage = window.postMessage;
var bridgeOrigin = location.protocol === 'file:' ? 'null' : location.origin;
var marker = document.createElement('script');
marker.type = 'application/crit-agent-marker';
marker.src = location.protocol === 'file:' ? 'data:text/javascript,/crit-agent.js' : location.origin + '/crit-agent.js';
marker.setAttribute('data-crit-cdp-agent', '1');
var nativeQuerySelectorAll = document.querySelectorAll;
document.querySelectorAll = function(selector) {
  if (selector === 'script[src*="/crit-agent.js"]') {
    var found = nativeQuerySelectorAll.call(this, selector);
    if (found.length) return found;
    return [marker];
  }
  return nativeQuerySelectorAll.apply(this, arguments);
};
function isAgentMessage(message) {
  var protocol = window.crit && window.crit.agentProtocol;
  if (!protocol || !protocol.validateMessage(message).ok) return false;
  for (var key in protocol.A2C) if (protocol.A2C[key] === message.type) return true;
  return false;
}
window.postMessage = function(message, targetOrigin) {
  if (targetOrigin === bridgeOrigin && isAgentMessage(message) && typeof window.__critCDPSend === 'function') {
    window.__critCDPSend(JSON.stringify(message));
    return;
  }
  return nativePostMessage.apply(this, arguments);
};
window.__critCDPReceive = function(message) {
  if (message && message.type === '` + CDPAgentHandshakeType + `') {
    if (typeof window.__critCDPSend === 'function') window.__critCDPSend('{"type":"agent-ready"}');
    return;
  }
  nativePostMessage.call(window, message, '*');
};
})();`

	css, err := fs.ReadFile(server.FrontendFS, "agent-marker.css")
	if err != nil {
		return "", "", fmt.Errorf("reading embedded agent-marker.css: %w", err)
	}
	cssJSON, _ := json.Marshal(string(css))
	bundle := "(function(){if(window.top !== window)return;\n" + bootstrap + "\n(function(){function inject(){if(document.querySelector('style[data-crit-marker-css=\"1\"]'))return;var root=document.head||document.documentElement;if(!root)return;var s=document.createElement('style');s.setAttribute('data-crit-marker-css','1');s.textContent=" + string(cssJSON) + ";root.appendChild(s);}inject();if(!document.documentElement)document.addEventListener('DOMContentLoaded',inject,{once:true});})();\n"
	for _, name := range server.AgentScriptFiles {
		script, readErr := fs.ReadFile(server.FrontendFS, name)
		if readErr != nil {
			return "", "", fmt.Errorf("reading embedded %s: %w", name, readErr)
		}
		bundle += "\n" + string(script) + "\n//# sourceURL=crit-cdp/" + name + "\n"
	}
	bundle += "\n})();"
	return bootstrap, bundle, nil
}
