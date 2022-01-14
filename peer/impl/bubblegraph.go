package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) processSplitEdgeMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing SplitEdge %v", n.Addr(), msg)
	sem := msg.(*types.SplitEdgeMessage)

	addr, ok := n.pickRandomPeer(pkt.Header.RelayedBy, pkt.Header.Source)
	if sem.TTL > 0 && ok {
		// transfer the message to a random neighbor
		return n.SendSplitEdge(pkt.Header.Source, addr, sem.TTL-1)
	}
	// if there no other neighbors than the sender and the relayer,
	// we are the receiver of the split edge

	// we are the source: resend a split message if possible
	// or just ignore
	if pkt.Header.Source == n.Addr() {
		if ok {
			err := n.SendSplitEdge(n.Addr(), addr, n.conf.BubbleGraphTTL)
			if err != nil {
				return err
			}
		}

		return nil
	}

	n.bubbleMutex.Lock()
	defer n.bubbleMutex.Unlock()

	helloMsg := types.ConnectionHelloMessage{}
	connHellomsg, err := n.conf.MessageRegistry.MarshalMessage(helloMsg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}

	_, ok = n.bubbleTable[pkt.Header.Source]

	// if the source is already a neighbor, send a hello to ensure synchronization
	// if we're not at the desired degree, just add the edge
	if ok || len(n.bubbleTable) != int(n.conf.BubbleGraphDegree) {
		err = n.Unicast(pkt.Header.Source, connHellomsg)
		if err != nil {
			return xerrors.Errorf("error sending a connection hello: %v", err)
		}

		n.bubbleTable[pkt.Header.Source] = struct{}{}
		return nil
	}

	// at this point we know that there are BubbleGraphDegree elements in BubbleTable
	addr, _ = n.pickRandomBubblePeer(false)

	// send redirect message to the selected former edge
	redirMsg := types.RedirectMessage{Split: pkt.Header.Source}
	connRedirMsg, err := n.conf.MessageRegistry.MarshalMessage(redirMsg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}
	err = n.Unicast(addr, connRedirMsg)
	if err != nil {
		return xerrors.Errorf("error sending a redirect: %v", err)
	}
	delete(n.bubbleTable, addr)

	// and send a hello to the source
	err = n.Unicast(pkt.Header.Source, connHellomsg)
	if err != nil {
		return xerrors.Errorf("error sending a connection hello: %v", err)
	}
	n.bubbleTable[pkt.Header.Source] = struct{}{}

	return nil
}

func (n *node) processRedirectMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing Redirect %v", n.Addr(), msg)
	redir := msg.(*types.RedirectMessage)

	n.bubbleMutex.Lock()
	defer n.bubbleMutex.Unlock()

	_, ok := n.bubbleTable[pkt.Header.Source]
	if !ok && len(n.bubbleTable) == int(n.conf.BubbleGraphDegree) {
		// the sender was not a neighbor, and we're full, so ignore the message
		return nil
	}

	delete(n.bubbleTable, pkt.Header.Source)
	if n.Addr() == redir.Split {
		return nil
	}

	helloMsg := types.ConnectionHelloMessage{}
	connHellomsg, err := n.conf.MessageRegistry.MarshalMessage(helloMsg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}

	err = n.Unicast(redir.Split, connHellomsg)
	if err != nil {
		return xerrors.Errorf("error sending a connection hello: %v", err)
	}
	n.bubbleTable[redir.Split] = struct{}{}

	return nil
}

func (n *node) processConnectionHelloMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing ConnectionHello %v", n.Addr(), msg)

	n.bubbleMutex.Lock()
	defer n.bubbleMutex.Unlock()

	_, ok := n.bubbleTable[pkt.Header.Source]
	if ok {
		return nil
	}

	if len(n.bubbleTable) != int(n.conf.BubbleGraphDegree) {
		n.bubbleTable[pkt.Header.Source] = struct{}{}
		return nil
	}

	// We cannot accept the incoming connection, reply with a Nope
	nopeMsg := types.ConnectionNopeMessage{}
	connNopemsg, err := n.conf.MessageRegistry.MarshalMessage(nopeMsg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}
	err = n.Unicast(pkt.Header.Source, connNopemsg)
	if err != nil {
		return xerrors.Errorf("error sending a connection nope: %v", err)
	}

	return nil
}

func (n *node) processConnectionNopeMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing ConnectionNope %v", n.Addr(), msg)

	n.bubbleMutex.Lock()
	defer n.bubbleMutex.Unlock()

	delete(n.bubbleTable, pkt.Header.Source)

	// we know that we are missing some edges, so send a SplitEdgeMessage to speed up
	// the building process (not have to wait for BubbleGraphLoop)
	peer, ok := n.pickRandomPeer()
	if !ok {
		return nil
	}

	return n.SendSplitEdge(n.Addr(), peer, n.conf.BubbleGraphTTL)
}

// Send a SplitEdgeMessage, from the given source, to the given dest, with the given TTL.
func (n *node) SendSplitEdge(source, dest string, TTL uint) error {
	log.Debug().Msgf("[%v] sending split edge message from %v to %v", n.addr, source, dest)
	msg := types.SplitEdgeMessage{
		TTL: TTL,
	}

	msg2, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}

	hdr := transport.NewHeader(source, n.addr, dest, 0)
	err = n.conf.Socket.Send(dest, transport.Packet{
		Header: &hdr,
		Msg:    &msg2,
	}, 0)
	if err != nil {
		return xerrors.Errorf("error sending packet: %v", err)
	}

	return nil
}
