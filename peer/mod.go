package peer

import (
	"time"

	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
)

// Peer defines the interface of a peer in the Peerster system. It embeds all
// the interfaces that will have to be implemented.
type Peer interface {
	Service
	Messaging
	DataSharing
	Torrent
}

// Factory is the type of function we are using to create new instances of
// peers.
type Factory func(Configuration) Peer

// Configuration if the struct that will contain the configuration argument when
// creating a peer. This struct will evolve.
type Configuration struct {
	Socket          transport.Socket
	MessageRegistry registry.Registry

	// AntiEntropyInterval is the interval at which the peer sends a status
	// message to a random neighbor. 0 means no status messages are sent.
	// Default: 0
	AntiEntropyInterval time.Duration

	// HeartbeatInterval is the interval at which a rumor with an EmptyMessage
	// is sent. At startup a rumor with EmptyMessage should always be sent. Note
	// that sending a rumor is expensive as it involve the
	// ack+status+continueMongering mechanism, which generates a lot of
	// messages. Having a low value can flood the system. A value of 0 means the
	// heartbeat mechanism is not activated, ie. no rumors with EmptyMessage are
	// sent at all.
	// Default: 0
	HeartbeatInterval time.Duration

	// AckTimeout is the timeout after which a peer consider a message lost. A
	// value of 0 represents an infinite timeout.
	// Default: 3s
	AckTimeout time.Duration

	// ContinueMongering defines the chance to send the rumor to a random peer
	// in case both peers are synced. 1 means it will continue, 0.5 means there
	// is a 50% chance, and 0 no chance.
	// Default: 0.5
	ContinueMongering float64

	// ChunkSize defines the size of chunks when storing data.
	// Default: 8192
	ChunkSize uint

	// Backoff parameters used for DataRequests.
	// Default: {2s 2 5}
	BackoffDataRequest Backoff

	Storage storage.Storage

	BubbleGraphDegree  uint
	BubbleGraphTTL     uint
	BubbleGraphTimeout time.Duration
}

// Backoff describes parameters for a backoff algorithm. The initial time must
// be multiplied by "factor" a maximum of "retry" time.
//   for i := 0; i < retry; i++ {
//     wait(initial)
//     initial *= factor
//   }
type Backoff struct {
	Initial time.Duration
	Factor  uint
	Retry   uint
}
