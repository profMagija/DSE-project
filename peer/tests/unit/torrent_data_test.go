package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
)

func Test_Torrent_Data_Download_0(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node2.UpdateCatalog("abc", node2.GetAddr())

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	err := node1.StartTorrent("abc")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	dlPeers := node1.GetDownloadingFrom("abc")

	require.Len(t, dlPeers, 1)
	require.Equal(t, dlPeers[0], node2.GetAddr())
}

func Test_Torrent_Data_Download_1(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	fileContents := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}, {'c', 'c', 'c'}}

	node2.UploadFile("abc", fileContents)

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	err := node1.StartTorrent("abc")
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	data := node1.GetFileParts("abc")
	require.Len(t, data, 3)
	require.Equal(t, data, fileContents)

}

func Test_Torrent_Data_Download_Three_Nodes(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	fileContents := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}, {'c', 'c', 'c'}}

	node3.UploadFile("abc", fileContents)

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	err := node2.StartTorrent("abc")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = node1.StartTorrent("abc")
	require.NoError(t, err)

	time.Sleep(1000 * time.Millisecond)

	require.Equal(t, fileContents, node1.GetFileParts("abc"))
	require.Equal(t, fileContents, node2.GetFileParts("abc"))
	require.Equal(t, fileContents, node3.GetFileParts("abc"))

}
