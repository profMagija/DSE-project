package impl

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	n := &node{
		stopped: 0,
		conf:    conf,
		addr:    conf.Socket.GetAddress(),

		rumourSeq: 0,

		routeTable:        make(map[string]string),
		status:            make(map[string]uint),
		savedRumors:       make(map[string][]types.Rumor),
		ackNotif:          NotifChan{},
		catalog:           make(peer.Catalog),
		torrentPeers:      make(map[string]map[string]struct{}),
		torrentPeerWd:     make(map[string]map[string]*Watchdog),
		torrentDataParts:  make(map[string][]TorrentDataPart),
		torrentFinishTime: make(map[string]time.Time),
	}

	n.routeTable[conf.Socket.GetAddress()] = conf.Socket.GetAddress()

	return n
}

type TorrentDataPart struct {
	Hash         []byte
	Data         []byte
	Downloading  string
	Availability map[string]struct{}
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer

	stopped uint64
	conf    peer.Configuration
	addr    string

	rumourSeq uint64

	statusLock  sync.RWMutex
	status      map[string]uint
	savedRumors map[string][]types.Rumor

	routeMutex sync.RWMutex
	routeTable map[string]string

	ackNotif    NotifChan
	searchNotif NotifChan

	searchDedup sync.Map

	cataLock sync.RWMutex
	catalog  peer.Catalog

	torrentLock       sync.RWMutex
	torrentPeers      map[string]map[string]struct{}
	torrentPeerWd     map[string]map[string]*Watchdog
	torrentDataParts  map[string][]TorrentDataPart
	torrentFinishTime map[string]time.Time
}

// Returns the current node address.
func (n *node) Addr() string {
	return n.addr
}

// Returns true if the node is running.
func (n *node) IsRunning() bool {
	return atomic.LoadUint64(&n.stopped) == 0
}

// Handles the given packet, if it is addressed to it.
// Otherwise relays it further.
// On error just logs it, does not panic.
func (n *node) handlePacket(pkt transport.Packet) {
	if pkt.Header.Destination == n.Addr() {
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Err(err).Msg("error processing packet")
		}
	} else {
		err := n.relayPacket(pkt)
		if err != nil {
			log.Err(err).Msg("error relaying packet")
		}
	}
}

// The anti-entropy loop. Only call this if AntiEntropyInterval != 0. Just sends a status to a random peer.
// Panics on all errors.
func (n *node) antiEntropyLoop() {
	for n.IsRunning() {
		time.Sleep(n.conf.AntiEntropyInterval)

		peer, ok := n.pickRandomPeer()
		if !ok {
			continue
		}

		err := n.sendStatusMessageTo(peer)
		if err != nil {
			panic(err)
		}
	}
}

// The heartbeat loop. Only call this if HeartbeatInterval != 0. Simply broadcasts an empty message every loop.
// Panics on all errors.
func (n *node) heartbeatLoop() {
	for n.IsRunning() {
		time.Sleep(n.conf.HeartbeatInterval)

		empty := types.EmptyMessage{}

		emptyMsg, err := n.conf.MessageRegistry.MarshalMessage(empty)
		if err != nil {
			panic(err)
		}

		err = n.Broadcast(emptyMsg)
		if err != nil {
			panic(err)
		}
	}
}

// The listener loop. Receives the packets, and spawns a handler goroutine.
// Panics on all errors.
func (n *node) listenerLoop() {
	for n.IsRunning() {
		pkt, err := n.conf.Socket.Recv(time.Second)
		if errors.Is(err, transport.TimeoutErr(0)) {
			continue
		} else if err != nil {
			panic(err)
		}

		go n.handlePacket(pkt)
	}
}

// Start implements peer.Service
//
// Starts the listener loop, anti-entropy and heartbeat loops (if requested) and registers all the message handlers.
func (n *node) Start() error {
	go n.listenerLoop()

	if n.conf.AntiEntropyInterval != 0 {
		go n.antiEntropyLoop()
	}

	if n.conf.HeartbeatInterval != 0 {
		go n.heartbeatLoop()
	}

	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.processChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.processRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.processStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.processAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.processPrivateMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, func(m types.Message, p transport.Packet) error { return nil })
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, n.processDataRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, n.processDataReplyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, n.processSearchRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, n.processSearchReplyMessage)

	n.conf.MessageRegistry.RegisterMessageCallback(types.InitialPeerSearchMessage{}, n.processInitialPeerSearchMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.InitialPeerResponseMessage{}, n.processInitialPeerResponseMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataQueryRequest{}, n.processDataQueryRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataQueryResponse{}, n.processDataQueryResponseMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataDownloadRequest{}, n.processDataDownloadRequest)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataDownloadResponse{}, n.processDataDownloadResponse)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PeerDiscoveryRequest{}, n.processPeerDiscoveryRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PeerDiscoveryResponse{}, n.processPeerDiscoveryResponseMessage)

	return nil
}

// Stop implements peer.Service
//
// Stops the node by incrementing the `stopped` counter.
func (n *node) Stop() error {
	atomic.AddUint64(&n.stopped, 1)
	return nil
}
