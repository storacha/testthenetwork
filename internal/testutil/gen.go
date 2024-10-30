package testutil

import (
	crand "crypto/rand"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"
)

// RandomCAR creates a CAR with a single block of random bytes of the specified
// size. It returns the link of the root block, the hash of the root block, the
// hash of the CAR itself and the bytes of the CAR.
func RandomCAR(t *testing.T, size int) (ipld.Link, multihash.Multihash, multihash.Multihash, []byte) {
	digest, bytes := RandomBytes(t, size)
	root := cidlink.Link{Cid: cid.NewCidV1(cid.Raw, digest)}
	r := car.Encode([]ipld.Link{root}, func(yield func(block.Block, error) bool) {
		yield(block.NewBlock(root, bytes), nil)
	})
	carBytes, err := io.ReadAll(r)
	require.NoError(t, err)
	carDigest, err := multihash.Sum(carBytes, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return root, digest, carDigest, carBytes
}

func RandomBytes(t *testing.T, size int) (multihash.Multihash, []byte) {
	bytes := make([]byte, size)
	_, err := crand.Read(bytes)
	require.NoError(t, err)
	digest, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return digest, bytes
}

func RandomPrincipal(t *testing.T) ucan.Principal {
	return RandomSigner(t)
}

func RandomSigner(t *testing.T) principal.Signer {
	id, err := signer.Generate()
	require.NoError(t, err)
	return id
}

var assignedPorts = map[int]struct{}{}

// RandomLocalURL finds a free port and will not generate another URL with the
// same port until test cleanup, even if no service is bound to it.
func RandomLocalURL(t *testing.T) url.URL {
	var port int
	for {
		port = GetFreePort(t)
		if _, ok := assignedPorts[port]; !ok {
			assignedPorts[port] = struct{}{}
			t.Cleanup(func() { delete(assignedPorts, port) })
			break
		}
	}
	pubURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	require.NoError(t, err)
	return *pubURL
}

func RandomCID(t *testing.T) ipld.Link {
	digest, _ := RandomBytes(t, 10)
	return cidlink.Link{Cid: cid.NewCidV1(cid.Raw, digest)}
}
