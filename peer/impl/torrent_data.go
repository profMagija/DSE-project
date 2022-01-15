package impl

import (
	"crypto/sha1"
	"math"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) StartTorrent(fileID string) error {

	psm := types.InitialPeerSearchMessage{
		FileID:     fileID,
		Originator: n.addr,
	}
	msg, err := n.conf.MessageRegistry.MarshalMessage(psm)
	if err != nil {
		return xerrors.Errorf("error marshaling search: %v", err)
	}
	err = n.Broadcast(msg)
	if err != nil {
		return xerrors.Errorf("error broadcasting search: %v", err)
	}

	return nil
}

func (n *node) GetDownloadingFrom(fileID string) []string {

	n.torrentLock.RLock()
	defer n.torrentLock.RUnlock()

	mm, hasMm := n.torrentPeers[fileID]
	if !hasMm {
		return nil
	}

	var res []string
	for p := range mm {
		res = append(res, p)
	}

	return res
}

func (n *node) processInitialPeerSearchMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing initialPeerSearch %v", n.Addr(), msg)
	ipsm, ok := msg.(*types.InitialPeerSearchMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	n.SetRoutingEntry(ipsm.Originator, pkt.Header.RelayedBy)

	_, hasCatalog := n.torrentDataParts[ipsm.FileID]
	if hasCatalog {
		log.Debug().Msgf("[%s] We have %s", n.addr, ipsm.FileID)
		// we have this file, reply!
		msg, err := n.conf.MessageRegistry.MarshalMessage(&types.InitialPeerResponseMessage{
			RequestID: ipsm.RequestID,
			FileID:    ipsm.FileID,
			NumParts:  uint(len(n.torrentDataParts[ipsm.FileID])),
		})
		if err != nil {
			return xerrors.Errorf("error marshaling response: %v", err)
		}
		err = n.Unicast(ipsm.Originator, msg)
		if err != nil {
			return xerrors.Errorf("error sending response: %v", err)
		}
	} else {
		// we do not have this file
		log.Debug().Msgf("[%s] We do not have %s", n.addr, ipsm.FileID)
	}
	return nil
}

func (n *node) processInitialPeerResponseMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing initialPeerResponse %v", n.Addr(), msg)
	iprm, ok := msg.(*types.InitialPeerResponseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	n.torrentLock.Lock()
	defer n.torrentLock.Unlock()

	// only add it if we do not have anyone for this file
	mm, hasMap := n.torrentPeers[iprm.FileID]

	if hasMap && len(mm) > 0 {
		return nil
	}

	n.torrentPeers[iprm.FileID] = make(map[string]struct{})

	n.initializeDownload(iprm.FileID, pkt.Header.Source, iprm.NumParts)

	go n.startDownloadingFrom(iprm.FileID, pkt.Header.Source)

	return nil
}

func (n *node) initializeDownload(fileID, firstPeer string, numParts uint) {
	n.torrentPeerWd[fileID] = make(map[string]*Watchdog)

	parts := make([]TorrentDataPart, numParts)
	for _, part := range parts {
		part.Availability = make(map[string]struct{})
	}
	n.torrentDataParts[fileID] = parts

	go n.peerDiscoveryLoop(fileID)
}

func (n *node) getRandomFilePeer(fileID string) (string, bool) {
	if len(n.torrentPeers[fileID]) == 0 {
		return "", false
	}

	peers := make([]string, 0, len(n.torrentPeers[fileID]))
	for k := range n.torrentPeers[fileID] {
		peers = append(peers, k)
	}

	i := rand.Intn(len(peers))
	return peers[i], true
}

func (n *node) peerDiscoveryLoop(fileID string) {
	for len(n.torrentPeers) < 5 {

		time.Sleep(100 * time.Millisecond)

		randPeer, ok := n.getRandomFilePeer(fileID)
		if !ok {
			continue
		}

		pdr := &types.PeerDiscoveryRequest{
			FileID:     fileID,
			Originator: n.addr,
			TTL:        32,
		}
		pdrMsg, err := n.conf.MessageRegistry.MarshalMessage(pdr)
		if err != nil {
			panic(err)
		}
		err = n.Unicast(randPeer, pdrMsg)
		if err != nil {
			panic(err)
		}

	}
}

func (n *node) processPeerDiscoveryRequestMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing PeerDiscoveryRequest %v", n.Addr(), msg)
	pdr, ok := msg.(*types.PeerDiscoveryRequest)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	n.SetRoutingEntry(pdr.Originator, pkt.Header.RelayedBy)

	ttl := pdr.TTL - 1

	if ttl != 0 {
		randPeer, ok := n.getRandomFilePeer(pdr.FileID)
		if !ok {
			return nil
		}

		pdr2 := &types.PeerDiscoveryRequest{
			FileID:     pdr.FileID,
			Originator: pdr.Originator,
			TTL:        ttl,
		}
		pdrMsg, err := n.conf.MessageRegistry.MarshalMessage(pdr2)
		if err != nil {
			return xerrors.Errorf("error marshaling forward: %v", err)
		}
		err = n.Unicast(randPeer, pdrMsg)
		if err != nil {
			return xerrors.Errorf("error sending forward: %v", err)
		}
	} else {
		pdr2 := &types.PeerDiscoveryResponse{
			FileID: pdr.FileID,
		}
		pdrMsg, err := n.conf.MessageRegistry.MarshalMessage(pdr2)
		if err != nil {
			return xerrors.Errorf("error marshaling response: %v", err)
		}
		err = n.Unicast(pdr.Originator, pdrMsg)
		if err != nil {
			return xerrors.Errorf("error sending response: %v", err)
		}
	}

	return nil
}

func (n *node) processPeerDiscoveryResponseMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing PeerDiscoveryResponse %v", n.Addr(), msg)
	pdr, ok := msg.(*types.PeerDiscoveryResponse)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	n.SetRoutingEntry(pkt.Header.Source, pkt.Header.RelayedBy)

	go n.startDownloadingFrom(pdr.FileID, pkt.Header.Source)

	return nil
}

func (n *node) startDownloadingFrom(fileID, peer string) {
	if peer == n.addr {
		return
	}
	if _, ok := n.torrentPeers[fileID][peer]; ok {
		log.Debug().Msgf("[%s] Already downloading %s from %s", n.addr, fileID, peer)
		return
	}
	log.Debug().Msgf("[%s] Starting to download %s from %s", n.addr, fileID, peer)
	n.torrentPeers[fileID][peer] = struct{}{}
	for {
		wd := NewWatchdog()
		n.torrentPeerWd[fileID][peer] = wd

		dqr := &types.DataQueryRequest{FileID: fileID}
		msg, err := n.conf.MessageRegistry.MarshalMessage(dqr)
		if err != nil {
			panic(err)
		}
		err = n.Unicast(peer, msg)
		if err != nil {
			panic(err)
		}

		wd.Start(100 * time.Millisecond)

		<-wd.GetChannel()

		haveAll := true
		for _, part := range n.torrentDataParts[fileID] {
			if part.Data == nil {
				haveAll = false
				break
			}
		}

		if haveAll {
			log.Debug().Msgf("[%s] Whole file %s downloaded", n.addr, fileID)
			break
		}
	}
}

func (n *node) processDataQueryRequestMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing DataQueryRequest %v", n.Addr(), msg)
	dqr, ok := msg.(*types.DataQueryRequest)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	_, hasPeer := n.torrentPeers[pkt.Header.Source][pkt.Header.Source]
	if !hasPeer {
		go n.startDownloadingFrom(dqr.FileID, pkt.Header.Source)
	}

	pts, hasPts := n.torrentDataParts[dqr.FileID]
	if !hasPts {
		log.Debug().Msgf("[%s] we do not have any parts", n.addr)
		return nil
	}

	var result [][2]uint

	haveLast := false
	start := uint(0)

	for part, partVal := range pts {
		if haveLast {
			if partVal.Data == nil {
				haveLast = false
				result = append(result, [2]uint{start, uint(part)})
			}
		} else {
			if partVal.Data != nil {
				haveLast = true
				start = uint(part)
			}
		}
	}

	if haveLast {
		result = append(result, [2]uint{start, uint(len(pts))})
	}

	resp := &types.DataQueryResponse{
		FileID:          dqr.FileID,
		AvailableBlocks: result,
	}
	log.Debug().Msgf("[%s] sending DQResp %v", n.addr, resp)
	respMsg, err := n.conf.MessageRegistry.MarshalMessage(resp)
	if err != nil {
		return xerrors.Errorf("error marshaling response: %v", err)
	}
	err = n.Unicast(pkt.Header.Source, respMsg)
	if err != nil {
		return xerrors.Errorf("error sending response: %v", err)
	}

	return nil
}

func (n *node) processDataQueryResponseMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing DataQueryResponse %v", n.Addr(), msg)
	dqr, ok := msg.(*types.DataQueryResponse)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	n.torrentPeerWd[dqr.FileID][pkt.Header.Source].Update()

	parts := n.torrentDataParts[dqr.FileID]

	leastCommonCount := math.MaxInt
	leastCommon := -1

	for _, bl := range dqr.AvailableBlocks {
		for i := bl[0]; i < bl[1]; i++ {
			log.Debug().Msgf("[%s] i=%d parts[i]=%v", n.addr, i, parts[i])
			if parts[i].Availability == nil {
				parts[i].Availability = make(map[string]struct{})
			}
			parts[i].Availability[pkt.Header.Source] = struct{}{}

			if parts[i].Data == nil && parts[i].Downloading == "" && len(parts[i].Availability) < leastCommonCount {
				leastCommonCount = len(parts[i].Availability)
				leastCommon = int(i)
			}
		}
	}

	if leastCommon == -1 {
		log.Debug().Msgf("[%s] nothing to download", n.addr)
		return nil
	}

	parts[leastCommon].Downloading = pkt.Header.Source

	ddr := &types.DataDownloadRequest{
		FileID: dqr.FileID,
		PartID: uint(leastCommon),
	}
	log.Debug().Msgf("[%s] sending data download request %v", n.addr, ddr)
	ddrMsg, err := n.conf.MessageRegistry.MarshalMessage(ddr)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}
	err = n.Unicast(pkt.Header.Source, ddrMsg)
	if err != nil {
		return xerrors.Errorf("error sending message: %v", err)
	}

	return nil
}

func (n *node) processDataDownloadRequest(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing DataDownloadRequest %v", n.Addr(), msg)
	ddr, ok := msg.(*types.DataDownloadRequest)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	pts, hasPts := n.torrentDataParts[ddr.FileID]
	if !hasPts {
		log.Debug().Msgf("[%s] we do not have any parts", n.addr)
		return nil
	}

	part := pts[ddr.PartID]

	data := part.Data

	if data == nil {
		return xerrors.Errorf("we do not have the specified part")
	}

	ddresp := &types.DataDownloadResponse{
		FileID: ddr.FileID,
		PartID: ddr.PartID,
		Data:   data,
	}
	ddrMsg, err := n.conf.MessageRegistry.MarshalMessage(ddresp)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}
	err = n.Unicast(pkt.Header.Source, ddrMsg)
	if err != nil {
		return xerrors.Errorf("error sending message: %v", err)
	}

	return nil
}

func (n *node) processDataDownloadResponse(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing DataDownloadResponse %v", n.Addr(), msg)
	ddr, ok := msg.(*types.DataDownloadResponse)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	n.torrentPeerWd[ddr.FileID][pkt.Header.Source].Update()
	defer n.torrentPeerWd[ddr.FileID][pkt.Header.Source].Stop()

	pts, hasPts := n.torrentDataParts[ddr.FileID]
	if !hasPts {
		log.Debug().Msgf("[%s] we do not have any parts", n.addr)
		return nil
	}

	// part := pts[ddr.PartID]
	pts[ddr.PartID].Data = ddr.Data
	pts[ddr.PartID].Downloading = ""

	log.Debug().Msgf("[%s] *** GOT %s[%d] YAY !! ", n.addr, ddr.FileID, ddr.PartID)

	return nil
}

func (n *node) UploadFile(fileID string, parts [][]byte) error {
	r := make([]TorrentDataPart, len(parts))

	log.Debug().Msgf("[%v] Uploading file %s of %d parts", n.addr, fileID, len(parts))

	for i, part := range parts {
		r[i] = TorrentDataPart{
			Data: part,
			Hash: sha1.New().Sum(part),

			Downloading: "",

			Availability: map[string]struct{}{
				n.addr: {},
			},
		}
	}

	n.torrentDataParts[fileID] = r
	n.torrentPeers[fileID] = make(map[string]struct{})
	n.torrentPeerWd[fileID] = make(map[string]*Watchdog)

	return nil
}

func (n *node) GetFileParts(fileID string) [][]byte {
	parts := n.torrentDataParts[fileID]

	if parts == nil {
		return nil
	}

	result := make([][]byte, len(parts))

	for i, part := range parts {
		result[i] = part.Data
	}

	return result
}
