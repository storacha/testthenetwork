package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-capabilities/pkg/assert"
	"github.com/storacha/go-capabilities/pkg/blob"
	"github.com/storacha/go-capabilities/pkg/claim"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/testthenetwork/internal/bootstrap"
	"github.com/storacha/testthenetwork/internal/digestutil"
	"github.com/storacha/testthenetwork/internal/testutil"
	"github.com/storacha/testthenetwork/internal/upload"
	"github.com/stretchr/testify/require"
)

func TestUpload(t *testing.T) {
	// Start IPNI ////////////////////////////////////////////////////////////////
	ipniFindURL := testutil.RandomLocalURL(t)
	ipniAnnounceURL := testutil.RandomLocalURL(t)

	fmt.Println("→ starting IPNI service")
	bootstrap.StartIPNIService(t, ipniAnnounceURL, ipniFindURL)
	fmt.Printf("✔ IPNI find and announce services running at %s and %s\n", ipniFindURL.String(), ipniAnnounceURL.String())

	// Start Indexing Service ////////////////////////////////////////////////////
	indexingID := testutil.RandomSigner(t)
	indexingURL := testutil.RandomLocalURL(t)

	fmt.Println("→ starting indexing service")
	bootstrap.StartIndexingService(t, indexingID, indexingURL, ipniFindURL, ipniAnnounceURL)
	fmt.Printf("✔ indexing service (%s) running at %s\n", indexingID.DID(), indexingURL.String())

	// Start Storage Node ////////////////////////////////////////////////////////
	storageID := testutil.RandomSigner(t)
	storageURL := testutil.RandomLocalURL(t)
	// proof storage node can invoke on indexing service
	storageIndexingProof := delegation.FromDelegation(
		testutil.Must(
			delegation.Delegate(
				indexingID,
				storageID,
				[]ucan.Capability[ucan.NoCaveats]{
					ucan.NewCapability(
						claim.CacheAbility,
						indexingID.DID().String(),
						ucan.NoCaveats{},
					),
				},
			),
		)(t),
	)
	fmt.Println("→ starting storage node")
	bootstrap.StartStorageNode(t, storageID, storageURL, ipniAnnounceURL, indexingID, indexingURL, storageIndexingProof)
	fmt.Printf("✔ storage node (%s) running at %s\n", storageID.DID(), storageURL.String())

	// Start Upload Service //////////////////////////////////////////////////////
	uploadID := testutil.RandomSigner(t)
	// proof upload service can invoke on storage node
	uploadStorageProof := delegation.FromDelegation(
		testutil.Must(
			delegation.Delegate(
				storageID,
				uploadID,
				[]ucan.Capability[ucan.NoCaveats]{
					ucan.NewCapability(
						blob.AllocateAbility,
						storageID.DID().String(),
						ucan.NoCaveats{},
					),
					ucan.NewCapability(
						blob.AcceptAbility,
						storageID.DID().String(),
						ucan.NoCaveats{},
					),
				},
			),
		)(t),
	)
	// proof upload service can invoke on indexing service
	uploadIndexingProof := delegation.FromDelegation(
		testutil.Must(
			delegation.Delegate(
				indexingID,
				uploadID,
				[]ucan.Capability[ucan.NoCaveats]{
					ucan.NewCapability(
						assert.EqualsAbility,
						indexingID.DID().String(),
						ucan.NoCaveats{},
					),
					ucan.NewCapability(
						assert.IndexAbility,
						indexingID.DID().String(),
						ucan.NoCaveats{},
					),
				},
			),
		)(t),
	)

	fmt.Println("→ creating upload service")
	uploadService := upload.NewService(t, upload.Config{
		ID:                   uploadID,
		StorageNodeID:        storageID,
		StorageNodeURL:       storageURL,
		StorageProof:         uploadStorageProof,
		IndexingServiceID:    indexingID,
		IndexingServiceURL:   indexingURL,
		IndexingServiceProof: uploadIndexingProof,
	})
	fmt.Printf("✔ upload service (%s) created\n", uploadID.DID())

	space := testutil.RandomPrincipal(t).DID()
	digest, data := testutil.RandomCAR(t, 256)
	size := uint64(len(data))

	fmt.Printf("→ performing blob/add with %s\n", digestutil.Format(digest))
	address := uploadService.BlobAdd(t, space, digest, size)
	fmt.Println("✔ blob/add success")
	if address != nil {
		fmt.Printf("→ performing http/put to %s\n", address.URL.String())
		putBlob(t, address.URL, address.Headers, data)
		fmt.Println("✔ http/put success")
	}

	fmt.Println("→ performing ucan/conclude for http/put")
	claim := uploadService.ConcludeHTTPPut(t, space, digest, size, address.Expires)
	fmt.Println("✔ ucan/conclude success")

	fmt.Println("→ decoding location commitment")
	nb, rerr := assert.LocationCaveatsReader.Read(claim.Capabilities()[0].Nb())
	require.NoError(t, rerr)
	fmt.Printf("✔ decode success - %s @ %s\n", claim.Capabilities()[0].Can(), nb.Location[0].String())

	fmt.Println("→ fetching blob")
	blobBytes := fetchBlob(t, nb.Location[0])
	blobDigest, err := multihash.Sum(blobBytes, multihash.SHA2_256, -1)
	require.NoError(t, err)
	require.Equal(t, digest, blobDigest)
	fmt.Println("✔ fetch success")
}

func putBlob(t *testing.T, location url.URL, headers http.Header, data []byte) {
	req, err := http.NewRequest("PUT", location.String(), bytes.NewReader(data))
	require.NoError(t, err)
	req.Header = headers

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode)
}

func fetchBlob(t *testing.T, location url.URL) []byte {
	req, err := http.Get(location.String())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, req.StatusCode)
	data, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	return data
}
