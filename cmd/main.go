package main

import (
	"context"
	"log"

	util "github.com/Scr3amz/perxTest/config"
	"github.com/Scr3amz/perxTest/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	conf,err := util.LoadConfig()
	if err!= nil {
        log.Fatal("Failed to load configuration\n",err)
    }
	reader := kafka.NewReader(&conf)
	writer := kafka.NewWriter(&conf)
	defer reader.Reader.Close()
	defer writer.Writer.Close()

	ctx := context.Background()
	messages := make(chan kafkago.Message,conf.MessageCount)
	commitedMessages := make(chan kafkago.Message,conf.MessageCount)

	g, ctx:= errgroup.WithContext(ctx) 
	

	g.Go(func() error {
		return reader.FetchMessages(ctx, messages)
	})
	g.Go(func() error {
		return writer.WriteMessages(ctx, messages, commitedMessages)
	})
	g.Go(func() error{
		return reader.CommitMessages(ctx, commitedMessages)
	})
		
	err = g.Wait()
	if err!= nil {
        log.Fatal(err)
    }


}