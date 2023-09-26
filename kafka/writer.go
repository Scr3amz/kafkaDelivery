package kafka

import (
	"context"

	util "github.com/Scr3amz/perxTest/config"
	kafkago "github.com/segmentio/kafka-go"
)

type Writer struct {
	Writer *kafkago.Writer
}

func NewWriter(conf *util.Config) *Writer {
	writer := &kafkago.Writer{
		Addr:  kafkago.TCP(conf.Broker),
		Topic: conf.OutputTopic,
	}
	return &Writer{Writer: writer}

}

func (w *Writer) WriteMessages(ctx context.Context, messages chan kafkago.Message, commitedMessagesCh chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-messages:
			err := w.Writer.WriteMessages(ctx, kafkago.Message{
				Value: message.Value,
			})
			if  err != nil {
				return err
			}
			select {
			case commitedMessagesCh <- message:
			case <-ctx.Done():
			}
		}
	}

}
