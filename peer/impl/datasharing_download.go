package impl

import (
	"strings"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

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

func (n *node) processDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	log.Debug().Msgf("[%v] Processing dataReply %v", n.Addr(), msg)
	drm, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	n.ackNotif.WriteChan(drm.RequestID, drm.Value)
	return nil
}

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
		n.ackNotif.CreateChan(rid)
		err = n.Unicast(p, trMsg)
		if err != nil {
			return nil, xerrors.Errorf("error sending request: %v", err)
		}
		res, ok := n.ackNotif.WaitChanTimeout(rid, timeout)
		if !ok {
			timeout = timeout * time.Duration(n.conf.BackoffDataRequest.Factor)
			continue
		}
		if res == nil {
			return nil, xerrors.Errorf("empty response")
		}
		return res.([]byte), nil
	}

	return nil, xerrors.Errorf("no response")
}

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

func (n *node) Download(metahash string) ([]byte, error) {
	metafileb, err := n.getChunk(metahash)
	if err != nil {
		return nil, xerrors.Errorf("error getting metafile: %v", err)
	}
	metafile := string(metafileb)
	chunkKeys := strings.Split(metafile, peer.MetafileSep)
	fileData := make([]byte, 0, len(chunkKeys)*int(n.conf.ChunkSize))
	for i, chunkKey := range chunkKeys {
		chunkData, err := n.getChunk(chunkKey)
		if err != nil {
			return nil, xerrors.Errorf("error getting chunk %d/%d: %v", i+1, len(chunkKeys), err)
		}
		fileData = append(fileData, chunkData...)
	}
	return fileData, nil
}
