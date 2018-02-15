package broker

import "time"

type (
	Publisher interface {
		Publish(subj string, data []byte) error
	}

	Subscriber interface {
		Subscribe(subj string, callback func(data []byte) []byte) (unsubscribe func() error, err error)
	}

	Requestor interface {
		Request(subj string, data []byte, timeout time.Duration) ([]byte, error)
	}

	Broker interface {
		Publisher
		Subscriber
		Requestor
	}
)
