package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	dbSubscriber, stop := NewSubscriber()
	defer stop()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		defer func() { err = fmt.Errorf("subscribe cahnges: %v", recover()) }()
		SubscribeToChanges(ctx, dbSubscriber)
		return err
	})
	g.Go(func() (err error) {
		defer func() { err = fmt.Errorf("run server: %v", recover()) }()
		RunServer(ctx, dbSubscriber.Notifications)
		return err
	})

	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}
}
