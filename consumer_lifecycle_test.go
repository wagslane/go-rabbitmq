package rabbitmq

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// lifecyclePeer uses an in-memory AMQP peer so startup and shutdown ordering
// can be tested without Docker, polling broker state, or parsing log messages.
type lifecyclePeer struct {
	releaseOnce sync.Once
	conn        net.Conn
	writeMu     sync.Mutex
	consumeOK   chan struct{}
	methods     chan lifecycleMethod
	refuse      lifecycleMethod
}
type lifecycleMethod struct{ class, method, channel uint16 }

func newLifecycleConsumer(t *testing.T, refuse lifecycleMethod, options ...func(*ConsumerOptions)) (*Consumer, *lifecyclePeer) {
	t.Helper()
	client, server := net.Pipe()
	peer := &lifecyclePeer{conn: server, consumeOK: make(chan struct{}), methods: make(chan lifecycleMethod, 128), refuse: refuse}
	t.Cleanup(func() { _ = server.Close() })
	go peer.serve()
	logger := simpleLogF(func(string, ...interface{}) {})
	conn, err := NewConn("amqp://guest:guest@localhost/", WithConnectionOptionsLogger(logger),
		WithConnectionOptionsBaseReconnectInterval(time.Hour),
		WithConnectionOptionsConfig(Config{Dial: func(string, string) (net.Conn, error) { return client, nil }}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	opts := []func(*ConsumerOptions){WithConsumerOptionsLogger(logger), WithConsumerOptionsConsumerName("test"),
		WithConsumerOptionsExchangeName("events"), WithConsumerOptionsExchangeDeclare, WithConsumerOptionsRoutingKey("key")}
	consumer, err := NewConsumer(conn, "queue", append(opts, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.releaseConsume(); consumer.Close() })
	return consumer, peer
}

func (p *lifecyclePeer) releaseConsume() { p.releaseOnce.Do(func() { close(p.consumeOK) }) }

func lifecycleShort(s string) []byte { return append([]byte{byte(len(s))}, s...) }
func (p *lifecyclePeer) writeFrame(kind byte, ch uint16, payload []byte) error {
	frame := binary.BigEndian.AppendUint16([]byte{kind}, ch)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = append(frame, 0xce)
	_, err := p.conn.Write(frame)
	return err
}
func (p *lifecyclePeer) writeMethod(ch, class, method uint16, args []byte) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	payload := binary.BigEndian.AppendUint16(nil, class)
	payload = binary.BigEndian.AppendUint16(payload, method)
	return p.writeFrame(1, ch, append(payload, args...))
}
func (p *lifecyclePeer) serve() {
	defer p.conn.Close()
	if _, err := io.ReadFull(p.conn, make([]byte, 8)); err != nil {
		return
	}
	// connection.start: version, empty properties, PLAIN mechanism, en_US locale.
	if err := p.writeMethod(0, 10, 10, []byte{0, 9, 0, 0, 0, 0, 0, 0, 0, 5, 'P', 'L', 'A', 'I', 'N', 0, 0, 0, 5, 'e', 'n', '_', 'U', 'S'}); err != nil {
		return
	}
	for {
		head := make([]byte, 7)
		if _, err := io.ReadFull(p.conn, head); err != nil {
			return
		}
		payload := make([]byte, int(binary.BigEndian.Uint32(head[3:]))+1)
		if _, err := io.ReadFull(p.conn, payload); err != nil {
			return
		}
		if head[0] != 1 || len(payload) < 5 {
			continue
		}
		m := lifecycleMethod{binary.BigEndian.Uint16(payload), binary.BigEndian.Uint16(payload[2:]), binary.BigEndian.Uint16(head[1:])}
		p.methods <- m
		if m.class == 60 && m.method == 20 {
			<-p.consumeOK
		}
		if m.class == p.refuse.class && m.method == p.refuse.method {
			args := binary.BigEndian.AppendUint16(nil, 403)
			args = append(args, lifecycleShort("ACCESS_REFUSED - lifecycle test")...)
			args = binary.BigEndian.AppendUint16(args, m.class)
			args = binary.BigEndian.AppendUint16(args, m.method)
			if p.writeMethod(m.channel, 20, 40, args) != nil {
				return
			}
			continue
		}
		var err error
		switch {
		case m.class == 10 && m.method == 11: // start-ok -> tune
			err = p.writeMethod(0, 10, 30, []byte{0, 0, 0, 2, 0, 0, 0, 0})
		case m.class == 10 && m.method == 40:
			err = p.writeMethod(0, 10, 41, []byte{0})
		case m.class == 20 && m.method == 10:
			err = p.writeMethod(m.channel, 20, 11, []byte{0, 0, 0, 0})
		case m.class == 60 && m.method == 10:
			err = p.writeMethod(m.channel, 60, 11, nil)
		case m.class == 40 && m.method == 10:
			err = p.writeMethod(m.channel, 40, 11, nil)
		case m.class == 50 && m.method == 10:
			args := append(lifecycleShort("queue"), make([]byte, 8)...)
			err = p.writeMethod(m.channel, 50, 11, args)
		case m.class == 50 && m.method == 20:
			err = p.writeMethod(m.channel, 50, 21, nil)
		case m.class == 60 && m.method == 20:
			err = p.writeMethod(m.channel, 60, 21, lifecycleShort("test"))
		case m.class == 20 && m.method == 40:
			err = p.writeMethod(m.channel, 20, 41, nil)
		case m.class == 10 && m.method == 50:
			_ = p.writeMethod(0, 10, 51, nil)
			return
		}
		if err != nil {
			return
		}
	}
}

func (p *lifecyclePeer) waitMethod(t *testing.T, class, method uint16) lifecycleMethod {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case m := <-p.methods:
			if m.class == class && m.method == method {
				return m
			}
		case <-timer.C:
			t.Fatalf("did not receive method %d.%d", class, method)
			return lifecycleMethod{}
		}
	}
}
func (p *lifecyclePeer) deliver(t *testing.T, channel uint16, tag uint64) {
	t.Helper()
	args := binary.BigEndian.AppendUint64(lifecycleShort("test"), tag)
	args = append(args, 0, 0) // not redelivered, default exchange
	args = append(args, lifecycleShort("key")...)
	if err := p.writeMethod(channel, 60, 60, args); err != nil {
		t.Fatal(err)
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	header := binary.BigEndian.AppendUint64([]byte{0, 60, 0, 0}, 1)
	header = append(header, 0, 0) // no properties
	if err := p.writeFrame(2, channel, header); err != nil {
		t.Fatal(err)
	}
	if err := p.writeFrame(3, channel, []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestWaitReadyRequiresConsumeOK(t *testing.T) {
	consumer, peer := newLifecycleConsumer(t, lifecycleMethod{})
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(func(Delivery) Action { return Ack }) }()
	peer.waitMethod(t, 60, 20)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := consumer.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("before consume-ok: %v", err)
	}
	// A canceled waiter must not cancel startup. Multiple later waiters all see
	// success even though this empty queue never delivers a message.
	peer.releaseConsume()
	ctx, cancelReady := context.WithTimeout(context.Background(), time.Second)
	defer cancelReady()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := consumer.WaitReady(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-runErr:
		t.Fatalf("Run exited after readiness: %v", err)
	default:
	}
	consumer.Close()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if err := consumer.WaitReady(ctx); err != nil {
		t.Fatalf("initial result changed after close: %v", err)
	}
}

func TestWaitReadyReportsInitialSetupErrors(t *testing.T) {
	for _, refuse := range []lifecycleMethod{{class: 60, method: 10}, {class: 40, method: 10}, {class: 50, method: 10}, {class: 50, method: 20}, {class: 60, method: 20}} {
		t.Run(refuseName(refuse), func(t *testing.T) {
			consumer, peer := newLifecycleConsumer(t, refuse)
			peer.releaseConsume()
			runErr := make(chan error, 1)
			go func() { runErr <- consumer.Run(func(Delivery) Action { return Ack }) }()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := consumer.WaitReady(ctx)
			var ae *amqp.Error
			if !errors.As(err, &ae) || ae.Code != 403 {
				t.Fatalf("readiness error = %v", err)
			}
			if got := <-runErr; got != err {
				t.Fatalf("Run error %v differs from readiness error %v", got, err)
			}
		})
	}
}
func refuseName(m lifecycleMethod) string {
	switch m.class {
	case 40:
		return "exchange"
	case 50:
		if m.method == 10 {
			return "queue"
		}
		return "binding"
	default:
		if m.method == 10 {
			return "qos"
		}
		return "consume"
	}
}

func TestWaitReadyClosedBeforeRun(t *testing.T) {
	consumer, _ := newLifecycleConsumer(t, lifecycleMethod{})
	consumer.Close()
	if err := consumer.WaitReady(context.Background()); !errors.Is(err, amqp.ErrClosed) {
		t.Fatal(err)
	}
	if err := consumer.Run(func(Delivery) Action { return Ack }); !errors.Is(err, amqp.ErrClosed) {
		t.Fatal(err)
	}
}
func TestWaitReadyRejectsNoWait(t *testing.T) {
	consumer, _ := newLifecycleConsumer(t, lifecycleMethod{}, WithConsumerOptionsConsumerNoWait)
	if err := consumer.WaitReady(context.Background()); err == nil {
		t.Fatal("NoWait cannot confirm readiness")
	}
}

func TestCloseWithDrainKeepsChannelOpenAndStopsHandlers(t *testing.T) {
	consumer, peer := newLifecycleConsumer(t, lifecycleMethod{}, WithConsumerOptionsForceShutdown)
	peer.releaseConsume()
	active := make(chan Delivery, 1)
	releaseHandler, allowHandler := lifecycleGate(t)
	runErr := make(chan error, 1)
	go func() {
		runErr <- consumer.Run(func(d Delivery) Action { active <- d; <-releaseHandler; return Manual })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := consumer.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	subscription := peer.waitMethod(t, 60, 20)
	peer.deliver(t, subscription.channel, 1)
	var delivery Delivery
	select {
	case delivery = <-active:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	draining := make(chan struct{})
	finishDrain, allowDrain := lifecycleGate(t)
	closed := make(chan struct{})
	go func() {
		consumer.CloseWithDrain(func() {
			close(draining)
			<-finishDrain
			if err := delivery.Ack(false); err != nil {
				t.Errorf("ack during drain: %v", err)
			}
		})
		close(closed)
	}()
	// Wait for shutdown admission to close, without depending on scheduling.
	for !consumer.getIsClosed() {
		select {
		case <-ctx.Done():
			t.Fatal("shutdown did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case <-draining:
		t.Fatal("drain ran before active handler finished")
	default:
	}
	allowHandler()
	select {
	case <-draining:
	case <-ctx.Done():
		t.Fatal("drain did not start")
	}
	peer.deliver(t, subscription.channel, 2)
	allowDrain()
	// No nack/requeue or channel close may precede the application's final ack.
	for {
		var m lifecycleMethod
		select {
		case m = <-peer.methods:
		case <-ctx.Done():
			t.Fatal("no final ack during drain")
		}
		if m.class == 60 && m.method == 80 {
			break
		}
		if (m.class == 20 && m.method == 40) || (m.class == 60 && m.method == 120) {
			t.Fatalf("method before final ack: %+v", m)
		}
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("CloseWithDrain did not finish")
	}
	select {
	case <-active:
		t.Fatal("handler invoked during drain")
	default:
	}
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	consumer.CloseWithDrain(func() { t.Error("drain ran twice") })
}

func lifecycleGate(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return gate, release
}
