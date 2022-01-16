package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Process the ACK message. ACKs the packet as received and processes the inner StatusMessage
func (n *node) processAckMessage(msg types.Message, pkt transport.Packet) error {
	// log.Debug().Msgf("[%v] Processing ack %v", n.Addr(), msg)
	ackM, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	var m types.Message = &ackM.Status

	n.ackNotif.WriteChan(ackM.AckedPacketID, struct{}{})

	err := n.processStatusMessage(m, pkt)
	if err != nil {
		return xerrors.Errorf("error processing ack status: %v", err)
	}

	return nil
}
