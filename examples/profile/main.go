package main

import (
	"fmt"

	"game-server/framework/runtime/profile"
)

func main() {
	profile.Init("examples/node/config.yaml")

	var base profile.NodeBaseConfig
	if err := profile.Get("base", &base); err != nil {
		panic(err)
	}

	fmt.Printf("self.id=%s\n", base.Self.GetID())
	fmt.Printf("nats.url=%s\n", base.Nats.URL)
	fmt.Printf("consul.addr=%s\n", base.Consul.Address)
}
