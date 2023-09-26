package kafka

import (
	"context"
	"log"

	util "github.com/Scr3amz/perxTest/config"
	kafkago "github.com/segmentio/kafka-go"
)

type Reader struct {
	Reader *kafkago.Reader
}

func NewReader(conf *util.Config) *Reader {

	reader := kafkago.NewReader(kafkago.ReaderConfig{
        Brokers: []string{conf.Broker},
        Topic:   conf.InputTopic,
		GroupID: "group1",
    })
    return &Reader{Reader: reader}
}

func (r *Reader) FetchMessages(ctx context.Context, messages chan<- kafkago.Message) error {
	for {
		msg, err := r.Reader.FetchMessage(ctx)
		if err!= nil {
			return err
		}
		select {
			case <-ctx.Done():
				return ctx.Err()
			case messages <- msg:
				log.Printf("message fetched and sent to a channel: %v \n", string(msg.Value))
		}
	}

}

func (r *Reader) CommitMessages(ctx context.Context, commitedMessagesCh <-chan kafkago.Message) error {
	for {
		select {
			case <-ctx.Done():
            case msg := <-commitedMessagesCh:
                err := r.Reader.CommitMessages(ctx, msg)
                if err!= nil {
                    return err
                }
				log.Printf("committed an msg: %v \n", string(msg.Value))
		}
	}
}





