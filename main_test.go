package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-capabilities/pkg/assert"
	"github.com/storacha/go-capabilities/pkg/blob"
	"github.com/storacha/go-capabilities/pkg/claim"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/testthenetwork/internal/bootstrap"
	"github.com/storacha/testthenetwork/internal/digestutil"
	"github.com/storacha/testthenetwork/internal/testutil"
	"github.com/storacha/testthenetwork/internal/upload"
	"github.com/stretchr/testify/require"
)

func TestUpload(t *testing.T) {
	logging.SetLogLevel("*", "warn")

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

	fmt.Println("→ creating indexing service client")
	indexingClient, err := client.New(indexingID, indexingURL)
	require.NoError(t, err)
	fmt.Printf("✔ indexing service client created\n")

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

	// Alice /////////////////////////////////////////////////////////////////////
	aliceID := testutil.RandomSigner(t)
	// proof upload service can invoke on indexing service
	aliceIndexingProof := delegation.FromDelegation(
		testutil.Must(
			delegation.Delegate(
				indexingID,
				aliceID,
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
		ID:             uploadID,
		StorageNodeID:  storageID,
		StorageNodeURL: storageURL,
		StorageProof:   uploadStorageProof,
	})
	fmt.Printf("✔ upload service (%s) created\n", uploadID.DID())

	space := testutil.RandomPrincipal(t).DID()

	fmt.Println("→ generating content")
	root, digest, data := testutil.RandomCAR(t, 256)
	rootDigest := digestutil.ExtractDigest(root)
	size := uint64(len(data))
	fmt.Printf("✔ generation success, root: %s\n", root.String())

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

	fmt.Println("→ creating index")
	index, err := blobindex.FromShardArchives(root, [][]byte{blobBytes})
	require.NoError(t, err)
	indexData, err := io.ReadAll(testutil.Must(index.Archive())(t))
	require.NoError(t, err)
	indexSize := uint64(len(indexData))
	indexDigest, err := multihash.Sum(indexData, multihash.SHA2_256, -1)
	require.NoError(t, err)
	indexLink := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), indexDigest)}
	fmt.Printf("✔ index created: %s (%s)\n", indexLink.String(), digestutil.Format(indexDigest))

	fmt.Printf("→ performing index blob/add with %s\n", digestutil.Format(indexDigest))
	address = uploadService.BlobAdd(t, space, indexDigest, indexSize)
	fmt.Println("✔ index blob/add success")

	if address != nil {
		fmt.Printf("→ performing index http/put to %s\n", address.URL.String())
		putBlob(t, address.URL, address.Headers, indexData)
		fmt.Println("✔ index http/put success")
	}

	fmt.Println("→ performing index ucan/conclude for http/put")
	uploadService.ConcludeHTTPPut(t, space, indexDigest, indexSize, address.Expires)
	fmt.Println("✔ index ucan/conclude success")

	fmt.Printf("→ performing assert/index with %s\n", indexLink.String())
	err = indexingClient.PublishIndexClaim(context.Background(), aliceID, assert.IndexCaveats{
		Content: root,
		Index:   indexLink,
	}, delegation.WithProof(aliceIndexingProof))
	require.NoError(t, err)
	fmt.Println("✔ assert/index success")

	fmt.Printf("→ performing query for %s (%s)\n", root.String(), digestutil.Format(rootDigest))
	res, err := indexingClient.QueryClaims(context.Background(), types.Query{
		Hashes: []multihash.Multihash{rootDigest},
	})
	require.NoError(t, err)
	fmt.Println("✔ query success")

	printQueryResults(t, res)
}

func printQueryResults(t *testing.T, results types.QueryResult) {
	blocks := map[ipld.Link]block.Block{}
	for b, err := range results.Blocks() {
		require.NoError(t, err)
		blocks[b.Link()] = b
	}
	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(results.Blocks()))
	require.NoError(t, err)

	fmt.Println("")
	fmt.Println("# Query Results")
	fmt.Println("")
	fmt.Printf("## Claims (%d)\n", len(results.Claims()))
	fmt.Println("")
	i := 1
	for _, link := range results.Claims() {
		fmt.Printf("%d. %s\n", i, link.String())
		claim, err := delegation.NewDelegationView(link, br)
		require.NoError(t, err)
		fmt.Printf("\tIssuer:   %s\n", claim.Issuer().DID())
		fmt.Printf("\tAudience: %s\n", claim.Audience().DID())
		cap := claim.Capabilities()[0]
		fmt.Printf("\tCan:      %s\n", cap.Can())
		fmt.Printf("\tWith:     %s\n", cap.With())
		switch cap.Can() {
		case assert.LocationAbility:
			fmt.Println("\tCaveats:")
			nb, err := assert.LocationCaveatsReader.Read(cap.Nb())
			require.NoError(t, err)
			fmt.Printf("\t\tSpace:    %s\n", nb.Space)
			fmt.Printf("\t\tContent:  %s\n", digestutil.Format(nb.Content.Hash()))
			fmt.Printf("\t\tLocation: %s\n", &nb.Location[0])
			if nb.Range != nil {
				if nb.Range.Length != nil {
					fmt.Printf("\t\tRange:    %d-%d\n", nb.Range.Offset, nb.Range.Offset+*nb.Range.Length)
				} else {
					fmt.Printf("\t\tRange:    %d-\n", nb.Range.Offset)
				}
			}
		case assert.IndexAbility:
			fmt.Println("\tCaveats:")
			nb, err := assert.IndexCaveatsReader.Read(cap.Nb())
			require.NoError(t, err)
			fmt.Printf("\t\tContent: %s\n", nb.Content.String())
			fmt.Printf("\t\tIndex:   %s\n", nb.Index.String())
		case assert.EqualsAbility:
			fmt.Println("\tCaveats:")
			nb, err := assert.EqualsCaveatsReader.Read(cap.Nb())
			require.NoError(t, err)
			fmt.Printf("\t\tContent: %s\n", digestutil.Format(nb.Content.Hash()))
			fmt.Printf("\t\tEquals:   %s\n", nb.Equals.String())
		}
		fmt.Println("")
		i++
	}
	fmt.Printf("## Indexes (%d)\n", len(results.Indexes()))
	fmt.Println("")
	i = 1
	for _, link := range results.Indexes() {
		fmt.Printf("%d. %s\n", i, link.String())
		b, ok, err := br.Get(link)
		require.NoError(t, err)
		require.True(t, ok)

		index, err := blobindex.Extract(bytes.NewReader(b.Bytes()))
		require.NoError(t, err)

		fmt.Printf("\tContent: %s\n", index.Content().String())
		fmt.Printf("\tShards (%d):\n", index.Shards().Size())
		for shard, slices := range index.Shards().Iterator() {
			fmt.Printf("\t\t%s\n", digestutil.Format(shard))
			for slice, position := range slices.Iterator() {
				fmt.Printf("\t\t\t%s %d-%d\n", digestutil.Format(slice), position.Offset, position.Offset+position.Length)
			}
		}
		fmt.Println("")
		i++
	}
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
