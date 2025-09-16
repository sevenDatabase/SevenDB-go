package main

import (
	"fmt"

	dicedb "github.com/sevenDatabase/SevenDB-go"
	"github.com/sevenDatabase/SevenDB-go/wire"
)

func main() {
	client, err := dicedb.NewClient("localhost", 7379)
	if err != nil {
		fmt.Println(err)
	}
	defer client.Close()

	resp := client.Fire(&wire.Command{Cmd: "PING"})
	fmt.Println(resp)
}
