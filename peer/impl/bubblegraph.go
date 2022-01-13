package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

func (n *node) processSplitEdgeMessage(msg types.Message, pkt transport.Packet) error {
	return nil
}

func (n *node) processRedirectMessage(msg types.Message, pkt transport.Packet) error {
	return nil
}

func (n *node) processConnectionHelloMessage(msg types.Message, pkt transport.Packet) error {
	return nil
}

func (n *node) processConnectionNopeMessage(msg types.Message, pkt transport.Packet) error {
	return nil
}
