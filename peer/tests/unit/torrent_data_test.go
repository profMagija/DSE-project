package unit

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
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

	node2.UploadFile("abc", nil)

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
	defer node3.Stop()

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

	df := node1.GetDownloadingFrom("abc")
	require.Len(t, df, 2)
	require.Contains(t, df, node2.GetAddr())
	require.Contains(t, df, node3.GetAddr())

}

func Test_Torrent_Data_Download_Perf(t *testing.T) {

	util := func(NUM_NODES, NUM_CONNS, NUM_BLOCKS int) {
		transp := channel.NewTransport()

		BLOCK_SIZE := 16

		var nodes []z.TestNode
		for i := 0; i < NUM_NODES; i++ {
			nodes = append(nodes, z.NewTestNode(t, peerFac, transp, "127.0.0.1:0"))
			defer nodes[i].Stop()

			if i < NUM_CONNS {
				for j := 0; j < i; j++ {
					nodes[i].AddPeer(nodes[j].GetAddr())
					nodes[j].AddPeer(nodes[i].GetAddr())

					// log.Debug().Msgf("%d <-> %d", i, j)
				}
			} else {
				for j := 0; j < NUM_CONNS; j++ {
					k := rand.Intn(i)
					nodes[i].AddPeer(nodes[k].GetAddr())
					nodes[k].AddPeer(nodes[i].GetAddr())
					// log.Debug().Msgf("%d <-> %d", i, k)
				}
			}
		}

		var fileData [][]byte

		for i := 0; i < NUM_BLOCKS; i++ {
			block := make([]byte, BLOCK_SIZE)
			fileData = append(fileData, block)
		}

		originator := rand.Intn(NUM_NODES)

		err := nodes[originator].UploadFile("abc", fileData)
		require.NoError(t, err)

		startTimes := make([]time.Time, NUM_NODES)
		for i := range nodes {
			if i == originator {
				continue
			}
			startTimes[i] = time.Now()
			err = nodes[i].StartTorrent("abc")
			require.NoError(t, err)
		}

		allFinished := false
		for !allFinished {
			time.Sleep(300 * time.Millisecond)
			allFinished = true
			for i := range nodes {
				if nodes[i].GetFinishTime("abc").Before(startTimes[i]) {
					allFinished = false
					break
				}
			}
		}

		var values []time.Duration
		for i := range nodes {
			if i == originator {
				continue
			}
			values = append(values, nodes[i].GetFinishTime("abc").Sub(startTimes[i]))
		}

		count := len(nodes) - 1

		sort.Slice(values, func(i, j int) bool {
			return values[i] < values[j]
		})
		
		// log.Debug().Msgf("%v", values)
		
		median := values[(count - 1) / 2]
		sum := time.Duration(0)
		for _, v := range values {
			sum += v
		}
		

		log.Debug().Msgf("RESULT: %d,%d,%d,%f,%f", NUM_NODES, NUM_CONNS, NUM_BLOCKS, float64(median.Nanoseconds()) / 1000000.0, float64(sum.Nanoseconds()) / float64(count) / 1000000.0)
	}

	for _, numNodes := range [...]int{8, 16, 32, 48, 64, 80} {
		for i := 0; i < 15; i++ {
			util(numNodes, 3, 16)
		}
	}

	// for _, numConns := range [...]int{3, 5, 10, 15, 20} {
	// 	for i := 0; i < 15; i++ {
	// 		util(48, numConns, 16)
	// 	}
	// }

}
