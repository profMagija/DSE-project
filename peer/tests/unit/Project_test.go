package unit

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
)

type graphT = [][]uint
type nodesT = []z.TestNode

func shift_graph(edges *graphT, n uint) {
	for i := range *edges {
		for j := range (*edges)[i] {
			(*edges)[i][j] += n
		}
	}
}

// generate a complete graph with n nodes
func generate_complete(n uint) graphT {
	edges := make(graphT, n)
	for i := range edges {
		edges[i] = make([]uint, n)
		for j := range edges {
			edges[i][j] = uint(j)
		}
	}
	return edges
}

func append_graph(edges1 graphT, edges2 graphT) graphT {
	shift_graph(&edges2, uint(len(edges1)))
	return append(edges1, edges2...)
}

func generate_empty() graphT {
	return make(graphT, 0)
}

func generate_topology(n uint) graphT {
	subsize := uint(4)
	graph := generate_empty()
	for i := uint(0); i < n/subsize; i++ {
		graph = append_graph(graph, generate_complete(subsize))
	}
	for i := uint(0); i < n/subsize; i++ {
		curri := i * subsize
		nexti := ((i + 1) * subsize) % n
		graph[curri] = append(graph[curri], nexti)
		graph[nexti] = append(graph[nexti], curri)
	}
	return graph
}

func build_nodes(t *testing.T, transp transport.Transport, edges graphT, opts []z.Option) nodesT {
	nodes := make([]z.TestNode, len(edges))

	for i := range nodes {
		nodes[i] = z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", opts...)
	}

	for i, neigh := range edges {
		for _, edge := range neigh {
			nodes[i].AddPeer(nodes[edge].GetAddr())
		}
	}

	return nodes
}

func evaluate_search_all(t *testing.T, transp transport.Transport, graph graphT) uint {
	chunkSize := uint(2)
	opts := []z.Option{
		z.WithChunkSize(chunkSize),
		// at least every peer will send a heartbeat message on start,
		// which will make everyone to have an entry in its routing
		// table to every one else, thanks to the antientropy.
		z.WithHeartbeat(time.Second * 200),
		z.WithAntiEntropy(time.Second),
		z.WithAckTimeout(time.Second * 10),
		z.WithBubbleGraph(5, 10, time.Millisecond*200),
	}

	nodes := build_nodes(t, transp, graph, opts)
	for _, n := range nodes {
		defer n.Stop()
	}

	time.Sleep(time.Second * 5)

	mhs := make([]string, len(nodes))
	for i, n := range nodes {
		file := []byte("lorem ipsum dolor sit amet")
		mh, err := n.Upload(bytes.NewBuffer(file))
		require.NoError(t, err)
		mhs[i] = mh
		err = n.Tag(fmt.Sprintf("file%d", i), mh)
		require.NoError(t, err)
	}

	names, err := nodes[0].SearchAll(*regexp.MustCompile("file.*"), uint(len(graph)), time.Second*time.Duration(len(graph)/4))
	sort.Slice(names, func(i, j int) bool {
		val1, _ := strconv.Atoi(names[i][4:])
		val2, _ := strconv.Atoi(names[j][4:])
		return val1 < val2
	})
	for _, name := range names {
		print(name, " ")
	}
	println()
	require.NoError(t, err)

	return uint(len(names))
}

func evaluate_search_first(t *testing.T, transp transport.Transport, graph graphT) time.Duration {
	chunkSize := uint(2)
	opts := []z.Option{
		z.WithChunkSize(chunkSize),
		// at least every peer will send a heartbeat message on start,
		// which will make everyone to have an entry in its routing
		// table to every one else, thanks to the antientropy.
		z.WithHeartbeat(time.Second * 200),
		z.WithAntiEntropy(time.Second),
		z.WithAckTimeout(time.Second * 10),
		z.WithBubbleGraph(5, 10, time.Millisecond*200),
	}

	nodes := build_nodes(t, transp, graph, opts)
	for _, n := range nodes {
		defer n.Stop()
	}

	time.Sleep(time.Second * 5)

	file := []byte("lorem ipsum dolor sit amet")
	mh, err := nodes[len(graph)/2].Upload(bytes.NewBuffer(file))
	require.NoError(t, err)
	err = nodes[len(graph)/2].Tag("file", mh)
	require.NoError(t, err)

	conf := peer.ExpandingRing{
		Initial: 4,
		Factor:  2,
		Retry:   100,
		Timeout: time.Millisecond * 500,
	}

	start := time.Now()
	_, err = nodes[0].SearchFirst(*regexp.MustCompile("file"), conf)
	elapsed := time.Since(start)
	require.NoError(t, err)

	return elapsed
}

func print_graph(graph graphT) {
	println("Size", len(graph))
	for i, edges := range graph {
		print(i, ":")
		for _, edge := range edges {
			print(" ", edge)
		}
		println()
	}
}

func Test_Project_SearchFirst(t *testing.T) {
	for _, n := range []uint{8, 16, 32, 48, 64, 80} {
		graph := generate_topology(n)
		// print_graph(graph)
		time := evaluate_search_first(t, udpFac(), graph)
		fmt.Printf("Evaluating SearchFirst with topology of size %d: took %d ns\n", n, time)
	}
}

func Test_Project_SearchAll(t *testing.T) {
	for _, n := range []uint{8, 16, 32, 48, 64, 80} {
		graph := generate_topology(n)
		// print_graph(graph)
		num := evaluate_search_all(t, udpFac(), graph)
		fmt.Printf("Evaluating SearchAll with topology of size %d: found %d\n", n, num)
	}
}
