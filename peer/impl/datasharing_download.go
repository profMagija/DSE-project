package impl

import (
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// process a DataRequestMessage.
func (n *node) processDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing dataRequest %v", n.Addr(), msg)
	drm, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	val := n.conf.Storage.GetDataBlobStore().Get(drm.Key)

	rmsg := types.DataReplyMessage{
		RequestID: drm.RequestID,
		Key:       drm.Key,
		Value:     val,
	}
	trRmsg, err := n.conf.MessageRegistry.MarshalMessage(rmsg)
	if err != nil {
		return xerrors.Errorf("error marshaling message: %v", err)
	}
	err = n.Unicast(pkt.Header.Source, trRmsg)
	if err != nil {
		return xerrors.Errorf("error sending reply: %v", err)
	}
	return nil
}

// Process a DataReplyMessage. Put the response into a corresponding channel.
func (n *node) processDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing dataReply %v", n.Addr(), msg)
	drm, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	n.ackNotif.WriteChan(drm.RequestID, drm.Value)
	return nil
}

// Download a given chunk. Picks a random neighbour from the catalog,
// and performs the exponential backoff.
func (n *node) downloadChunk(key string) ([]byte, error) {
	p, ok := n.getCatalogRandomPeer(key)
	if !ok {
		return nil, xerrors.Errorf("file not found: %v", key)
	}

	timeout := n.conf.BackoffDataRequest.Initial
	for i := uint(0); i < n.conf.BackoffDataRequest.Retry; i++ {
		rid := xid.New().String()
		msg := types.DataRequestMessage{
			RequestID: rid,
			Key:       key,
		}
		trMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
		if err != nil {
			return nil, xerrors.Errorf("error marshaling message: %v", err)
		}
		// create the response channel
		n.ackNotif.CreateChan(rid)
		// send the message
		err = n.Unicast(p, trMsg)
		if err != nil {
			return nil, xerrors.Errorf("error sending request: %v", err)
		}
		// get the response
		res, ok := n.ackNotif.WaitChanTimeout(rid, timeout)
		if !ok {
			// we did not get the response, retry
			timeout = timeout * time.Duration(n.conf.BackoffDataRequest.Factor)
			continue
		}
		if res == nil {
			// we got an empty response
			return nil, xerrors.Errorf("empty response")
		}
		// we got data
		return res.([]byte), nil
	}

	// we failed enough times
	return nil, xerrors.Errorf("no response")
}

// get a single chunk. Either queries local store, or calls "downloadChunk"
func (n *node) getChunk(key string) ([]byte, error) {
	val := n.conf.Storage.GetDataBlobStore().Get(key)
	if val != nil {
		return val, nil
	}

	val, err := n.downloadChunk(key)
	if err != nil {
		return nil, xerrors.Errorf("error fetching chunk: %v", err)
	}

	return val, nil
}

// implements peer.DataSharing
func (n *node) Download(metahash string) ([]byte, error) {
	// metafileb, err := n.getChunk(metahash)
	// if err != nil {
	// 	return nil, xerrors.Errorf("error getting metafile: %v", err)
	// }
	// metafile := string(metafileb)
	// chunkKeys := strings.Split(metafile, peer.MetafileSep)
	// fileData := make([]byte, 0, len(chunkKeys)*int(n.conf.ChunkSize))
	// for i, chunkKey := range chunkKeys {
	// 	chunkData, err := n.getChunk(chunkKey)
	// 	if err != nil {
	// 		return nil, xerrors.Errorf("error getting chunk %d/%d: %v", i+1, len(chunkKeys), err)
	// 	}
	// 	fileData = append(fileData, chunkData...)
	// }
	// return fileData, nil

	if n.torrentDataParts[metahash] == nil {
		err := n.StartTorrent(metahash)
		log.Debug().Msgf("[%s] Started torrent %s", n.addr, metahash)
		if err != nil {
			return nil, xerrors.Errorf("error starting torrent: %v", err)
		}
		return nil, nil
	}

	for _, part := range n.torrentDataParts[metahash] {
		if part.Data == nil {
			log.Debug().Msgf("[%s] torrent in progress %s", n.addr, metahash)
			return nil, nil
		}
	}
	log.Debug().Msgf("[%s] torrent complete %s", n.addr, metahash)

	result := make([]byte, 0)
	for _, part := range n.torrentDataParts[metahash] {
		result = append(result, part.Data...)
	}
	
	return result, nil
}
