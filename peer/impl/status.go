package impl

import (
	"math/rand"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Process the status message from a peer. The algorithm is as follows:
//
// 1. compare the messages
// 2. if we have some messages peer does not, send all of them in a rumor
// 3. if peer has some messages we do not, send them a status, so they can send them back (by 2. on their side)
// 4. if neither 2. nor 3. is true, perform the "Continue Mongering" procedure
func (n *node) processStatusMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing status %v", n.Addr(), msg)
	theirStatus, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	myStatus := n.copyStatus()
	sender := pkt.Header.Source

	// ensure sender is our peer
	if !n.isPeer(sender) && sender != n.Addr() && sender != n.Addr() {
		n.AddPeer(sender)
	}

	// compare our statuses from one and the other side
	theyKnow := n.checkIfPeerKnowsMore(theirStatus, myStatus)
	toSend := n.checkIfWeKnowMode(theirStatus, myStatus)

	if toSend != nil {
		// there are messages we should send to the peer
		err := n.sendRumorsToPeer(sender, toSend)
		if err != nil {
			return xerrors.Errorf("error sending rumors to peer: %v", err)
		}
	}

	if theyKnow {
		// we should request them to send us some messages
		err := n.sendStatusMessageTo(sender)
		if err != nil {
			return err
		}
	}

	if !theyKnow && toSend == nil {
		// nothing to exchange, continue mongering
		err := n.continueMongering(sender)
		if err != nil {
			return xerrors.Errorf("error mongering: %v", err)
		}
	}

	return nil
}

// Check if the peer (`theirStatus`) has some extra messages not in `myStatus` (based on sequence numbers).
func (n *node) checkIfPeerKnowsMore(theirStatus *types.StatusMessage, myStatus types.StatusMessage) bool {
	for origin, seq := range *theirStatus {
		mySeq, ok := myStatus[origin]

		if !ok {
			mySeq = 0
		}

		if seq > mySeq {
			log.Debug().Msgf("[%v] they have %v %v, (i have %v)", n.Addr(), origin, seq, mySeq)
			return true
		}
	}

	return false
}

// Check and get all messages that we have (`myStatus`) not in `theirStatus` (based on sequence numbers).
// Returns a list of all messages that should be sent to the peer, in sequence number order (for each origin),
// or `nil` if no such message exists.
func (n *node) checkIfWeKnowMode(theirStatus *types.StatusMessage, myStatus types.StatusMessage) []types.Rumor {
	var toSend []types.Rumor = nil
	for origin, seq := range myStatus {
		theirSeq, ok := (*theirStatus)[origin]
		if !ok {
			theirSeq = 0
		}

		if theirSeq < seq {
			n.statusLock.RLock()
			toSend = append(toSend, n.savedRumors[origin][theirSeq:]...)
			n.statusLock.RUnlock()
		}
	}

	return toSend
}

// Send a list of rumors to the peer.
func (n *node) sendRumorsToPeer(peer string, toSend []types.Rumor) error {
	log.Debug().Msgf("[%v] sending %v rumors to %v", n.Addr(), len(toSend), peer)
	rums := types.RumorsMessage{
		Rumors: toSend,
	}

	rumsMsg, err := n.conf.MessageRegistry.MarshalMessage(rums)
	if err != nil {
		return err
	}
	err = n.Unicast(peer, rumsMsg)
	if err != nil {
		return err
	}

	return nil
}

// Send a status message to a peer.
func (n *node) sendStatusMessageTo(peer string) error {
	var status types.StatusMessage = n.copyStatus()

	rumSeq := atomic.LoadUint64(&n.rumourSeq)
	if rumSeq > 0 {
		status[n.Addr()] = uint(rumSeq)
	}

	statusMsg, err := n.conf.MessageRegistry.MarshalMessage(status)
	if err != nil {
		return err
	}

	hdr := transport.NewHeader(n.Addr(), n.Addr(), peer, 0)
	err = n.conf.Socket.Send(peer, transport.Packet{
		Header: &hdr,
		Msg:    &statusMsg,
	}, 0)

	return err
}

// Continue mongering procedure. The algorithm is as follows.
// 1. With some probability p (set as conf.ContinueMongering) continue, otherwise stop.
// 2. Pick a random peer, other than `originator` (the original status sender). If no peer exists, stop.
// 3. Send that peer a status message.
func (n *node) continueMongering(originator string) error {
	log.Debug().Msgf("[%v] continuing mongering", n.Addr())
	if rand.Float64() < n.conf.ContinueMongering {
		peer, hasPeer := n.pickRandomPeer(originator)
		if hasPeer {
			log.Debug().Msgf("[%v] sending status to %v", n.Addr(), peer)
			err := n.sendStatusMessageTo(peer)
			if err != nil {
				return err
			}
		} else {
			log.Debug().Msgf("[%v] no one to monger to", n.Addr())
		}
	} else {
		log.Debug().Msgf("[%v] failed monger check, prob = %v", n.Addr(), n.conf.ContinueMongering)
	}

	return nil
}

// Copy the node status. Required to prevent data races.
func (n *node) copyStatus() map[string]uint {
	n.statusLock.RLock()
	defer n.statusLock.RUnlock()

	res := make(map[string]uint)

	for k, v := range n.status {
		res[k] = v
	}

	return res
}
