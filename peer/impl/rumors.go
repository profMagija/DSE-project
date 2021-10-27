package impl

import (
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Processes the RumorsMessage. Processes the internal rumors, ACKs the message, and then (if any rumors were processed)
// spreads the rumor further.
func (n *node) processRumorsMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing rumors %v", n.Addr(), msg)
	rumors, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	sender := pkt.Header.Source

	anyProcessedRumors, err := n.processRumorsInternal(rumors, pkt)
	if err != nil {
		return xerrors.Errorf("error processing rumors: %v", err)
	}

	// no need to send ack to self
	if sender != n.Addr() {
		go n.sendRumorAck(sender, pkt)
	} else {
		log.Debug().Msgf("[%v] self message, not acking", n.Addr())
	}

	if anyProcessedRumors {
		go n.spreadRumor(sender, pkt.Msg)
	}

	return nil
}

// Send the ACK to the sender for the given packet. Panics on error.
func (n *node) sendRumorAck(sender string, pkt transport.Packet) {
	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        n.copyStatus(),
	}

	log.Debug().Msgf("[%v] sending ack to %v", n.Addr(), sender)
	ackMsg, err := n.conf.MessageRegistry.MarshalMessage(ack)
	if err != nil {
		panic(xerrors.Errorf("error marshaling message: %v", err))
	}
	hdr := transport.NewHeader(n.Addr(), n.Addr(), sender, 0)
	ackPkt := transport.Packet{
		Header: &hdr,
		Msg:    &ackMsg,
	}
	err = n.conf.Socket.Send(sender, ackPkt, 0)
	if err != nil {
		panic(xerrors.Errorf("error sending ack: %v", err))
	}
}

// Process the expected rumors in the message. The unexpected (duplicated or out-of-order) rumors are ignored.
// Returns true if any messages were processed. Errors if any message processing errors as well.
func (n *node) processRumorsInternal(rumors *types.RumorsMessage, pkt transport.Packet) (bool, error) {
	n.statusLock.Lock()
	defer n.statusLock.Unlock()

	anyProcessedRumors := false

	for _, rum := range rumors.Rumors {
		prevStat, hasPrevStat := n.status[rum.Origin]
		if (!hasPrevStat && rum.Sequence == 1) || (hasPrevStat && rum.Sequence == prevStat+1) {
			err := n.processSingleRumorInternal(rum, pkt)
			if err != nil {
				return false, xerrors.Errorf("error processing rumor: %v", err)
			}
			anyProcessedRumors = true
		} else {
			log.Debug().Msgf("[%v] expected %v got %v from %v status=%v", n.Addr(), prevStat+1, rum.Sequence, rum.Origin, n.status)
		}
	}

	return anyProcessedRumors, nil
}

// Processes a single rumor message, by constructing an equivalent packet, and processing that using the MessageRegistry.
// Also adds the routing info for the originator of the rumor, if we have no info about it.
func (n *node) processSingleRumorInternal(rum types.Rumor, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] received %v from %v", n.Addr(), rum.Sequence, rum.Origin)
	n.status[rum.Origin] = rum.Sequence
	n.savedRumors[rum.Origin] = append(n.savedRumors[rum.Origin], rum)
	hdr := transport.NewHeader(rum.Origin, pkt.Header.RelayedBy, pkt.Header.Destination, 0)
	err := n.conf.MessageRegistry.ProcessPacket(transport.Packet{
		Header: &hdr,
		Msg:    rum.Msg,
	})
	if err != nil {
		return xerrors.Errorf("error processing packet: %v", err)
	}

	if !n.hasRoutingInfo(rum.Origin) {
		n.SetRoutingEntry(rum.Origin, pkt.Header.RelayedBy)
	}

	log.Debug().Msgf("[%v] done processing", n.Addr())
	return nil
}

// Spread the given message to a random peer. We will keep retrying to send to a peer until we either succeed
// (which includes geting an ack back), or we run out of peers. Never sends to the original sender.
func (n *node) spreadRumor(sender string, msg *transport.Message) {
	except := make([]string, 1)
	except[0] = sender
	for {
		peer, ok := n.pickRandomPeer(except...)
		if ok {
			log.Debug().Msgf("[%v] resending rumor to %v (excluding=%v)", n.Addr(), peer, except)
			hdr2 := transport.NewHeader(n.Addr(), n.Addr(), peer, 0)
			pkt2 := transport.Packet{
				Header: &hdr2,
				Msg:    msg,
			}

			ackCh := n.createAckChannel(hdr2.PacketID)

			err := n.conf.Socket.Send(peer, pkt2, n.conf.AckTimeout)
			if err != nil {
				log.Debug().Msgf("[%v] sending failed with %v, retrying with different peer ...", n.Addr(), err)
				except = append(except, peer)
				continue
			}

			if n.waitForAck(hdr2.PacketID, n.conf.AckTimeout, ackCh) {
				log.Debug().Msgf("[%v] rumor acked", n.Addr())
				return
			}

			log.Debug().Msgf("[%v] ack timeout reached, retrying with different peer ...", n.Addr())
			except = append(except, peer)
		} else {
			log.Debug().Msgf("[%v] no peers to send to :( received from %v", n.Addr(), sender)
			return
		}
	}
}

// Create and return an ACK channel for a given packetId.
// The channel should be written to when a message is ACKed, and read from when waiting for an ack.
// The channel is buffered so that the ACK can be registered without blocking.
// NOTE: be sure to create the channel before sending the packet.
func (n *node) createAckChannel(packetId string) chan struct{} {
	n.ackChanLock.Lock()
	defer n.ackChanLock.Unlock()

	ch := make(chan struct{}, 1)
	n.ackChanMap[packetId] = ch
	return ch
}

// Delete the ACK channel for the given packetId.
// This prevents a memory leak that would otherwise occur.
// The channel should be deleted when an ack is received or a timeout occurs.
func (n *node) deleteAckChannel(packetId string) {
	n.ackChanLock.Lock()
	defer n.ackChanLock.Unlock()

	delete(n.ackChanMap, packetId)
}

// ACK a packet. Writes to the corresponding ack channel. Returns
// true if the packet is ACKed, and false if the timeout already
// expired (because the channel is deleted).
func (n *node) ackPacket(packetId string) bool {
	n.ackChanLock.RLock()
	defer n.ackChanLock.RUnlock()

	ch, ok := n.ackChanMap[packetId]
	if !ok {
		return false
	}

	ch <- struct{}{}
	return true
}

// Wait for the packet ACK. Returns `true` if the ACK has been received, and `false` if timeout
// expired. A timeout of 0 indicates no timeout (the function will wait forever).
// Deletes the ACK channel to free memory.
func (n *node) waitForAck(packetId string, timeout time.Duration, ch chan struct{}) bool {
	success := false

	if timeout == 0 {
		<-ch
		success = true
	} else {
		select {
		case <-ch:
			success = true
		case <-time.After(timeout):
		}
	}

	n.deleteAckChannel(packetId)
	return success
}
