package impl

import (
	"crypto"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"golang.org/x/xerrors"
)

// Reads from `data` until `buf` is full, or EOF is reached.
// If returned amount is less than the length of the buffer,
// end is reached. Any non-EOF error is propagated.
func readExact(data io.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := data.Read(buf[total:])
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			} else {
				return 0, err
			}
		}
	}
	return total, nil
}

// Hashes, and returns the hex-encoded as well as original
// hash value
func hashF(b []byte) (string, []byte) {
	h := crypto.SHA256.New()
	h.Write(b)
	ms := h.Sum(nil)
	return hex.EncodeToString(ms), ms
}

// implements peer.DataSharing
func (n *node) Upload(data io.Reader) (metahash string, err error) {
	log.Debug().Msgf("[%v] starting upload", n.addr)
	store := n.conf.Storage.GetDataBlobStore()
	meta := ""
	metaKeyBytes := make([]byte, 0)
	for {
		buf := make([]byte, n.conf.ChunkSize)
		chunkSize, err := readExact(data, buf)
		if err != nil {
			return "", xerrors.Errorf("error reading data: %v", err)
		}
		if chunkSize == 0 {
			break
		}
		chunkData := buf[:chunkSize]
		
		chunkHash, chunkHashBytes := hashF(chunkData)
		store.Set(chunkHash, chunkData)
		log.Debug().Msgf("[%v] chunk %v -> %v", n.addr, chunkData, chunkHash)
		
		if len(meta) != 0 {
			meta += peer.MetafileSep
		}
		meta += chunkHash
		metaKeyBytes = append(metaKeyBytes, chunkHashBytes...)
	}
	
	log.Debug().Msgf("[%v] metafile: %v", n.addr, meta)
	
	metahash, _ = hashF(metaKeyBytes)
	store.Set(metahash, []byte(meta))
	
	return
}

// implements peer.DataSharing
func (n *node) Tag(name string, mh string) error {
	storage := n.conf.Storage.GetNamingStore()
	storage.Set(name, []byte(mh))
	return nil
}

// implements peer.DataSharing
func (n *node) Resolve(name string) (metahash string) {
	storage := n.conf.Storage.GetNamingStore()
	v := storage.Get(name)
	if v == nil {
		return ""
	} else {
		return string(v)
	}
}

// get a random peer from the catalog that contains the given hash
func (n *node) getCatalogRandomPeer(key string) (string, bool) {
	n.cataLock.RLock()
	defer n.cataLock.RUnlock()
	
	m, ok := n.catalog[key]
	if !ok || len(m) == 0 {
		return "", false
	}
	
	cur := ""
	count := 0
	for p := range m {
		count += 1
		if rand.Intn(count) == 0 {
			cur = p
		}
	}
	return cur, true
}

// implements peer.DataSharing
func (n *node) GetCatalog() peer.Catalog {
	return n.catalog
}

// implements peer.DataSharing
func (n *node) UpdateCatalog(key string, peer string) {
	n.cataLock.Lock()
	defer n.cataLock.Unlock()
	
	m, ok := n.catalog[key]
	if !ok {
		m = make(map[string]struct{})
		n.catalog[key] = m
	}
	m[peer] = struct{}{}
}
