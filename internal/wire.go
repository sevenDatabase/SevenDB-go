package internal

import "github.com/sevenDatabase/SevenDB-go/wire"

type Wire interface {
	Send([]byte) *wire.WireError
	Receive() ([]byte, *wire.WireError)
	Close()
}
