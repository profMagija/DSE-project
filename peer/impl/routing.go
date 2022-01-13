package impl

import (
	"math/rand"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"golang.org/x/xerrors"
)

// AddPeer implements peer.Service
//
// Adds peers to the node by setting `routeTable[addr] = addr`
func (n *node) AddPeer(addr ...string) {
	for _, a := range addr {
		n.SetRoutingEntry(a, a)
	}
}

// GetRoutingTable implements peer.Service
//
// Gets a copy of the routing table from the node.
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.routeMutex.RLock()
	defer n.routeMutex.RUnlock()

	result := make(map[string]string)
	for key, value := range n.routeTable {
		result[key] = value
	}

	return result
}

// SetRoutingEntry implements peer.Service
//
// Sets the route node for the given address `origin` to `relayAddr`.
// If `relayAddr == ""` removes the routing information instead.
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if origin == n.Addr() {
		return
	}

	n.routeMutex.Lock()
	defer n.routeMutex.Unlock()

	if relayAddr == "" {
		delete(n.routeTable, origin)
	} else {
		n.routeTable[origin] = relayAddr
	}
}

// Relays the packet to its target, by looking up the routing information.
// Errors if there is no route to the host.
func (n *node) relayPacket(pkt transport.Packet) error {

	dest := pkt.Header.Destination
	nextHop, ok := n.determineNextHop(dest)
	if !ok {
		return xerrors.Errorf("no route to target: %s", dest)
	}
	log.Debug().Msgf("Relaying to %s via %s", dest, nextHop)

	pkt.Header.RelayedBy = n.Addr()

	err := n.conf.Socket.Send(nextHop, pkt, 0)
	if err != nil {
		return xerrors.Errorf("could not send packet: %v", err)
	}

	return nil
}

// Determines the next hop to the router, by looking up the route table
func (n *node) determineNextHop(dest string) (string, bool) {
	n.routeMutex.RLock()
	defer n.routeMutex.RUnlock()

	res, ok := n.routeTable[dest]
	return res, ok
}

// Returns true if we know a route to the given destination.
func (n *node) hasRoutingInfo(dest string) bool {
	n.routeMutex.RLock()
	defer n.routeMutex.RUnlock()

	_, ok := n.routeTable[dest]
	return ok
}

// A simple membership test for arrays.
func memberOf(n string, ns []string) bool {
	for _, x := range ns {
		if n == x {
			return true
		}
	}
	return false
}

// Picks a random peer from all the peers of the node, except the ones
// listed in the arguments (if any). Also ignores self.
// Returns (peer, true) if one is found, or ("", false) if no other peers
// exist.
func (n *node) pickRandomPeer(except ...string) (string, bool) {
	n.routeMutex.RLock()
	defer n.routeMutex.RUnlock()

	cur := ""
	count := 0
	for relay, p := range n.routeTable {
		if p != n.Addr() && relay == p && !memberOf(p, except) {
			count += 1
			if rand.Intn(count) == 0 {
				cur = p
			}
		}
	}

	return cur, count > 0
}

// Same as above, but for the bubble graph table
// lock specifies whether the mutex has to be locked
func (n *node) pickRandomBubblePeer(lock bool, except ...string) (string, bool) {
	if lock {
		n.bubbleMutex.RLock()
		defer n.bubbleMutex.RUnlock()
	}

	cur := ""
	count := 0
	for relay := range n.bubbleTable {
		if !memberOf(relay, except) {
			count += 1
			if rand.Intn(count) == 0 {
				cur = relay
			}
		}
	}

	return cur, count > 0
}

// Returns true if the given address is a peer (because routeTable[addr] == addr).
// The node is never its own peer.
func (n *node) isPeer(addr string) bool {
	n.routeMutex.RLock()
	defer n.routeMutex.RUnlock()
	return addr != n.Addr() && n.routeTable[addr] == addr
}

// Get all peers (except the ones in the arguments), in a random permutation.
func (n *node) getPeerPermutation(except ...string) []string {
	n.routeMutex.RLock()
	defer n.routeMutex.RUnlock()

	res := make([]string, 0)
	for relay, p := range n.routeTable {
		if p != n.addr && relay == p && !memberOf(p, except) {
			res = append(res, p)
		}
	}

	rand.Shuffle(len(res), func(i, j int) {
		tmp := res[i]
		res[i] = res[j]
		res[j] = tmp
	})

	return res
}
