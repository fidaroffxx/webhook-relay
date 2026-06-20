package main

import "github.com/fidaroffxx/webhook-relay/internal/kernel"

func main() {
	newKernel := kernel.NewKernel()

	err := newKernel.Load()
	if err != nil {
		panic(err)
	}

	newKernel.Serve()
}
