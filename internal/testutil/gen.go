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

func RandomCAR(t *testing.T, size int) (multihash.Multihash, []byte) {
	digest, bytes := RandomBytes(t, size)
	root := cidlink.Link{Cid: cid.NewCidV1(cid.Raw, digest)}
	r := car.Encode([]ipld.Link{root}, func(yield func(block.Block, error) bool) {
		yield(block.NewBlock(root, bytes), nil)
	})
	carBytes, err := io.ReadAll(r)
	require.NoError(t, err)
	carDigest, err := multihash.Sum(carBytes, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return carDigest, carBytes
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

func RandomLocalURL(t *testing.T) url.URL {
	port, err := GetFreePort()
	require.NoError(t, err)
	pubURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	require.NoError(t, err)
	return *pubURL
}

func RandomCID(t *testing.T) ipld.Link {
	digest, _ := RandomBytes(t, 10)
	return cidlink.Link{Cid: cid.NewCidV1(cid.Raw, digest)}
}
