// Command demo is a minimal demonstration of the consistenthash package:
// it adds a few built-in nodes, routes fixed keys, removes a node, and
// shows how the keys are re-routed.
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
		ring.Add(node)
	}
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	fmt.Printf("members: %v (nodes=%d replicas=%d)\n",
		ring.Members(), ring.Len(), ring.Replicas())
	printRoutes(ring, keys)

	fmt.Println("\nremove node-b:", ring.Remove("node-b"))
	fmt.Printf("members: %v (nodes=%d)\n", ring.Members(), ring.Len())
	printRoutes(ring, keys)
}

func printRoutes(ring *consistenthash.Ring, keys []string) {
	for _, key := range keys {
		node, ok := ring.Get(key)
		if !ok {
			fmt.Printf("  %-8s -> <empty ring>\n", key)
			continue
		}
		fmt.Printf("  %-8s -> %s\n", key, node)
	}
}
