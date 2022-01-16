package impl

import (
	"sync/atomic"

	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Unicast implements peer.Messaging
//
// Sends the given message to a destination.
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.Addr(), n.Addr(), dest, 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	if err := n.relayPacket(pkt); err != nil {
		return xerrors.Errorf("error unicast routing: %v", err)
	}

	return nil
}

// Broadcast implements peer.Messaging
//
// Broadcasts the given message to all nodes on the network
func (n *node) Broadcast(msg transport.Message) error {
	// log.Debug().Msgf("broadcasting message %v", msg)

	rm := types.RumorsMessage{
		Rumors: make([]types.Rumor, 1),
	}

	rm.Rumors[0] = types.Rumor{
		Origin:   n.Addr(),
		Sequence: uint(atomic.AddUint64(&n.rumourSeq, 1)),
		Msg:      &msg,
	}

	rmMsg, err := n.conf.MessageRegistry.MarshalMessage(rm)
	if err != nil {
		return xerrors.Errorf("error marshaling rumor: %v", err)
	}

	header := transport.NewHeader(n.Addr(), n.Addr(), n.Addr(), 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &rmMsg,
	}

	err = n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		return xerrors.Errorf("error processing packet: %v", err)
	}

	return nil
}
