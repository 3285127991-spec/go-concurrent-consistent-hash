// Command demo is a minimal demonstration of the consistenthash package:
// it adds a few built-in nodes, routes fixed keys, removes a node, and
// shows how the same keys are re-routed.
package main

import (
	"fmt"
	"log"

	"consistenthash"
)

func main() {
	ring, err := consistenthash.New(100)
	if err != nil {
		log.Fatal(err)
	}

	for _, node := range []string{"node-a", "node-b", "node-c"} {
		if err := ring.Add(node); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("members:", ring.Members())

	keys := []string{"user-1", "order-42", "session-abc", "device-9", "cart-17"}
	route := func(label string) {
		fmt.Println(label)
		for _, key := range keys {
			node, err := ring.Get(key)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("  " + key + " -> " + node)
		}
	}

	route("routing:")

	if err := ring.Remove("node-b"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("removed node-b; members:", ring.Members())

	route("routing after removal:")
}
