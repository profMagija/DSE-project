package impl

import (
	"crypto/sha1"
	"math"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) StartTorrent(fileID string) error {
	reqId := xid.New().String()
	budget := 100

	go n.screamInitialSearch(reqId, uint(budget), fileID, n.Addr())

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

func (n *node) screamInitialSearch(requestID string, budget uint, fileID, originator string, except ...string) error {
	peers := n.getPeerPermutation(except...) // all peers except the one we just got the message from
	remaining := uint(len(peers))
	for _, p := range peers {
		sendBudget := budget / remaining
		remaining--
		if sendBudget == 0 {
			continue
		}

		budget -= sendBudget

		msg, err := n.conf.MessageRegistry.MarshalMessage(&types.InitialPeerSearchMessage{
			RequestID:  requestID,
			Budget:     sendBudget,
			FileID:     fileID,
			Originator: originator,
		})
		if err != nil {
			return xerrors.Errorf("error marshaling search: %v", err)
		}
		err = n.Unicast(p, msg)
		if err != nil {
			return xerrors.Errorf("error sending search: %v", err)
		}
	}

	return nil
}

func (n *node) processInitialPeerSearchMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing initialPeerSearch %v", n.Addr(), msg)
	ipsm, ok := msg.(*types.InitialPeerSearchMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	c := n.GetCatalog()
	catalog, hasCatalog := c[ipsm.FileID]
	if hasCatalog {
		_, hasSelf := catalog[n.Addr()]
		if hasSelf {
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
			return nil
		}
	}

	// we do not have this file

	return n.screamInitialSearch(
		ipsm.RequestID,
		ipsm.Budget-1,
		ipsm.FileID,
		ipsm.Originator,
		pkt.Header.Source,
	)
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
	n.torrentPeers[iprm.FileID][pkt.Header.Source] = struct{}{}

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
}

func (n *node) startDownloadingFrom(fileID, peer string) {
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

	log.Debug().Msgf("[%s] *** GOT %s[%d] : %v YAY !! ", n.addr, ddr.FileID, ddr.PartID, pts[ddr.PartID].Data)

	return nil
}

func (n *node) UploadFile(fileID string, parts [][]byte) error {
	r := make([]TorrentDataPart, len(parts))

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
	n.UpdateCatalog(fileID, n.addr)

	n.torrentDataParts[fileID] = r

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
