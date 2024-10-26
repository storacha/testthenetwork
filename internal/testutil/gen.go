package testutil

import (
	crand "crypto/rand"
	"fmt"
	"net/url"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"
)

func GenBytes(t *testing.T, size int) (multihash.Multihash, []byte) {
	bytes := make([]byte, size)
	_, err := crand.Read(bytes)
	require.NoError(t, err)
	digest, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return digest, bytes
}

func GenPrincipal(t *testing.T) ucan.Principal {
	return GenSigner(t)
}

func GenSigner(t *testing.T) principal.Signer {
	id, err := signer.Generate()
	require.NoError(t, err)
	return id
}

func GenURL(t *testing.T) url.URL {
	port, err := GetFreePort()
	require.NoError(t, err)
	pubURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	require.NoError(t, err)
	return *pubURL
}
