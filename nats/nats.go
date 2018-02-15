package nats

import (
	"github.com/Puppollo/broker"
	n "github.com/nats-io/go-nats"
	"time"
)

const defaultPoolSize = 100

type (
	// private struct for nats.Message processing
	msg struct {
		reply string
		data  []byte
	}

	worker struct{} // worker struct for handling messages

	Config struct {
		DSN                string `yaml:"DSN"`  // nats://<user>:<password>@<host>:<port>
		Name               string `yaml:"Name"` // subscriber name for debugging
		PoolSize           int    `yaml:"PoolSize"`
		ProcessOnThreshold bool   `yaml:"ProcessOnThreshold"` // if true - try to process all incoming messages, if false - process messages only if got free worker
	}

	Broker struct {
		Config

		connection *n.Conn
		pool       chan *worker
	}
)

func newMsg(m *n.Msg) msg {
	return msg{
		data:  m.Data,
		reply: m.Reply,
	}
}

// handle subscriber callback
func (worker) handle(m msg, cb func([]byte) []byte, publisher broker.Publisher) {
	data := cb(m.data)
	if m.reply != "" && data != nil {
		publisher.Publish(m.reply, data)
	}
}

func New(c *Config) (*Broker, error) {
	b := Broker{}
	if c != nil {
		b.Config = *c
	}
	var err error
	b.connection, err = n.Connect(b.DSN, n.Name(b.Config.Name))
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if b.PoolSize <= 0 {
		b.PoolSize = defaultPoolSize
	}
	// pool initialize
	b.pool = make(chan *worker, b.PoolSize)
	for i := 0; i < b.PoolSize; i++ {
		b.pool <- &worker{}
	}

	return &b, nil
}

func (b *Broker) Close() error {
	close(b.pool)
	b.connection.Close()
	return nil
}

// Subscribe for subjects, if callback return not nil, then publish to message.reply
func (b *Broker) Subscribe(subject string, callback func([]byte) []byte) (unsubscribe func() error, err error) {
	sub, err := b.connection.Subscribe(subject, func(m *n.Msg) {
		if len(b.pool) == 0 && !b.ProcessOnThreshold {
			// no free workers
			// TODO logging
			return
		}
		wrkr := <-b.pool // lock before worker are freed
		go func(w *worker) {
			if w == nil {
				// pool closed
				return
			}
			w.handle(newMsg(m), callback, b)
			b.pool <- w // put worker back to pool
		}(wrkr)
	})
	if err != nil {
		return nil, err
	}
	return sub.Unsubscribe, nil
}

// sync request for subject
func (b *Broker) Request(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	msg, err := b.connection.Request(subject, data, timeout)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (b *Broker) Publish(subject string, data []byte) error {
	return b.connection.Publish(subject, data)
}
