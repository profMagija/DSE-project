package impl

import (
	"regexp"
	"strings"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Process a SearchRequestMessage. Sends the file information back as a response,
// as well as forwarding the request further.
func (n *node) processSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing searchRequest %v", n.Addr(), msg)
	srm, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	n.SetRoutingEntry(srm.Origin, pkt.Header.RelayedBy)

	_, loaded := n.searchDedup.LoadOrStore(srm.RequestID, struct{}{})
	if loaded {
		return nil
	}

	reg, err := regexp.Compile(srm.Pattern)
	if err != nil {
		return xerrors.Errorf("error compiling regexp: %v", err)
	}

	fis := n.constructFileInfos(*reg)

	err = n.sendSearchResponse(srm.Origin, pkt.Header.Source, srm.RequestID, fis)
	if err != nil {
		return xerrors.Errorf("error sending search response: %v", err)
	}

	err = n.screamSearchRequest(srm.Origin, srm.Budget-1, *reg, func() string { return srm.RequestID }, pkt.Header.Source)
	if err != nil {
		return xerrors.Errorf("error screaming: %v", err)
	}

	return nil
}

// Processes a SearchReplyMessage. Updates name store and the catalog. Writes all
// response file infos to the corresponding channel.
func (n *node) processSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing searchReply %v", n.Addr(), msg)
	srm, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	n.SetRoutingEntry(pkt.Header.Source, pkt.Header.RelayedBy)

	for _, resp := range srm.Responses {
		n.Tag(resp.Name, resp.Metahash)
		n.UpdateCatalog(resp.Metahash, pkt.Header.Source)
		for _, ch := range resp.Chunks {
			if ch != nil {
				n.UpdateCatalog(string(ch), pkt.Header.Source)
			}
		}

		n.searchNotif.WriteChan(srm.RequestID, resp)
	}

	return nil
}

// Sends the search response. The response is always sent directly to the relay.
func (n *node) sendSearchResponse(origin, relay, rid string, fis []types.FileInfo) error {
	log.Debug().Msgf("[%v] sending search resp to %v via %v", n.addr, origin, relay)
	msg := types.SearchReplyMessage{
		RequestID: rid,
		Responses: fis,
	}

	msg2, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}

	hdr := transport.NewHeader(n.addr, n.addr, origin, 0)
	err = n.conf.Socket.Send(relay, transport.Packet{
		Header: &hdr,
		Msg:    &msg2,
	}, 0)
	if err != nil {
		return xerrors.Errorf("error sending packet: %v", err)
	}

	return nil
}

// Returns if the file info is "fully known" by the sender
// (has no nil-entry chunks)
func fullyKnown(fi types.FileInfo) bool {
	for _, r := range fi.Chunks {
		if r == nil {
			return false
		}
	}

	return true
}

// Constructs all the FileInfo-s the node knows about.
func (n *node) constructFileInfos(reg regexp.Regexp) []types.FileInfo {
	fis := make([]types.FileInfo, 0)
	nstore := n.conf.Storage.GetNamingStore()
	dstore := n.conf.Storage.GetDataBlobStore()

	nstore.ForEach(func(key string, val []byte) bool {
		if !reg.Match([]byte(key)) {
			// does not match the regex
			return true
		}

		fi := types.FileInfo{
			Name:     key,
			Metahash: string(val),
		}

		metafileBytes := dstore.Get(string(val))
		if metafileBytes == nil {
			// we do not have the metafile
			return true
		}
		metafile := string(metafileBytes)
		chunkKeys := strings.Split(metafile, peer.MetafileSep)
		fi.Chunks = make([][]byte, len(chunkKeys))

		for i, chunkKey := range chunkKeys {
			if dstore.Get(chunkKey) != nil {
				fi.Chunks[i] = []byte(chunkKey)
			}
		}
		fis = append(fis, fi)

		return true
	})

	return fis
}

// Send the search request to peer.
func (n *node) sendSearchRequest(peer string, budget uint, reg regexp.Regexp, origin, reqId string) error {
	log.Debug().Msgf("[%v] sending search to %v (%d)", n.addr, peer, budget)
	msg := types.SearchRequestMessage{
		RequestID: reqId,
		Origin:    origin,
		Pattern:   reg.String(),
		Budget:    budget,
	}

	msg2, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}

	hdr := transport.NewHeader(n.addr, n.addr, peer, 0)
	err = n.conf.Socket.Send(peer, transport.Packet{
		Header: &hdr,
		Msg:    &msg2,
	}, 0)
	if err != nil {
		return xerrors.Errorf("error sending packet: %v", err)
	}

	return nil
}

// Get all the names in the local store that match the regexp.
func (n *node) getNamesMatching(reg regexp.Regexp) []string {
	names := make([]string, 0)
	store := n.conf.Storage.GetNamingStore()
	store.ForEach(func(key string, val []byte) bool {
		if reg.Match([]byte(key)) {
			names = append(names, key)
		}
		return true
	})

	return names
}

// "Scream" the search request to all neighbours (except the ones noted in "exceptPeer").
// The `namer` is called before each request send, and should return the requestID.
// Budget is divided equally among the peers.
func (n *node) screamSearchRequest(origin string, budget uint, reg regexp.Regexp, namer func() string, exceptPeers ...string) error {
	peers := n.getBubblePeerPermutation(exceptPeers...)
	remaining := uint(len(peers))

	for _, p := range peers {
		sendBudget := budget / remaining
		remaining--
		if sendBudget == 0 {
			continue
		}
		budget -= sendBudget

		reqName := namer()

		err := n.sendSearchRequest(p, sendBudget, reg, origin, reqName)
		if err != nil {
			return xerrors.Errorf("error sending search request: %v", err)
		}
	}
	return nil
}

// implements peer.DataSharing
func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) ([]string, error) {
	// perform local search
	names := n.getNamesMatching(reg)

	// Send the request to all neighbours. For each sent request, we
	// create a new notification channel.
	reqNames := make([]string, 0)
	err := n.screamSearchRequest(n.addr, budget, reg, func() string {
		name := xid.New().String()
		reqNames = append(reqNames, name)
		n.searchNotif.CreateChanWithSize(name, 10)
		return name
	})
	if err != nil {
		return nil, xerrors.Errorf("error screaming: %v", err)
	}

	// wait for all responses to be recv'd
	time.Sleep(timeout)

	// read all the data from each channel, and place the names into
	// "names" array
	for _, rn := range reqNames {
		data := n.searchNotif.ReadWholeChan(rn)
		for _, di := range data {
			d := di.(types.FileInfo)
			if !memberOf(d.Name, names) {
				names = append(names, d.Name)
			}
		}
	}

	return names, nil
}

// implements peer.DataSharing
func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	// perform local search
	fis := n.constructFileInfos(pattern)
	for _, fi := range fis {
		if fullyKnown(fi) {
			return fi.Name, nil
		}
	}

	// the retry loop
	budget := conf.Initial
	for i := uint(0); i < conf.Retry; i++ {
		reqName := xid.New().String()
		n.searchNotif.CreateChanWithSize(reqName, 10)
		// scream the search request to all neighbours
		err := n.screamSearchRequest(n.addr, budget, pattern, func() string { return reqName })
		if err != nil {
			return "", xerrors.Errorf("error searching: %v", err)
		}

		// wait for the responses to be received
		time.Sleep(conf.Timeout)

		// read all responses
		for _, res := range n.searchNotif.ReadWholeChan(reqName) {
			fi := res.(types.FileInfo)
			if fullyKnown(fi) {
				return fi.Name, nil
			}
		}

		// we found nothing, retry
		budget *= conf.Factor
	}

	return "", nil
}
