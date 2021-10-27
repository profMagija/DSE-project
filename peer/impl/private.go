package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Processes the private message. Handles the inner message if the
// node is a recipient of the packet, otherwise ignores it.
func (n *node) processPrivateMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing private %v", n.Addr(), msg)
	privM, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	_, isRec := privM.Recipients[n.Addr()]
	if !isRec {
		return nil
	}

	pkt2 := transport.Packet{
		Header: pkt.Header,
		Msg:    privM.Msg,
	}

	err := n.conf.MessageRegistry.ProcessPacket(pkt2)
	if err != nil {
		return xerrors.Errorf("error processing private packet: %v", err)
	}
	return nil
}
