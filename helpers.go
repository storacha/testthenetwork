package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-capabilities/pkg/assert"
	"github.com/storacha/go-capabilities/pkg/blob"
	"github.com/storacha/go-capabilities/pkg/claim"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
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

func generateURLs(t *testing.T) (ipniFindURL, ipniAnnounceURL, storageURL, indexingURL url.URL) {
	ipniFindURL = testutil.RandomLocalURL(t)
	ipniAnnounceURL = testutil.RandomLocalURL(t)
	storageURL = testutil.RandomLocalURL(t)
	indexingURL = testutil.RandomLocalURL(t)
	return
}

func generateIdentities(t *testing.T) (storageID, indexingID, uploadID, aliceID, bobID principal.Signer) {
	storageID = testutil.RandomSigner(t)
	indexingID = testutil.RandomSigner(t)
	uploadID = testutil.RandomSigner(t)
	aliceID = testutil.RandomSigner(t)
	bobID = testutil.RandomSigner(t)
	return
}

func generateProofs(t *testing.T, storageID, indexingID principal.Signer, uploadID, aliceID, bobID ucan.Principal) (storageIndexingProof, uploadStorageProof, aliceIndexingProof, bobIndexingProof delegation.Proof) {
	// proof storage node can invoke on indexing service
	storageIndexingProof = delegation.FromDelegation(
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
				delegation.WithNoExpiration(),
			),
		)(t),
	)
	// proof upload service can invoke on storage node
	uploadStorageProof = delegation.FromDelegation(
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
				delegation.WithNoExpiration(),
			),
		)(t),
	)
	// proof alice can invoke on indexing service
	aliceIndexingProof = delegation.FromDelegation(
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
				delegation.WithNoExpiration(),
			),
		)(t),
	)
	// proof bob can invoke on indexing service
	bobIndexingProof = delegation.FromDelegation(
		testutil.Must(
			delegation.Delegate(
				indexingID,
				bobID,
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
				delegation.WithNoExpiration(),
			),
		)(t),
	)
	return
}

func startServices(
	t *testing.T,
	ipniFindURL, ipniAnnounceURL url.URL,
	storageID principal.Signer,
	storageURL url.URL,
	storageIndexingProof delegation.Proof,
	indexingID principal.Signer,
	indexingURL url.URL,
	indexingNoCache bool,
	uploadID principal.Signer,
	uploadStorageProof delegation.Proof,
) (*upload.UploadService, *client.Client) {
	fmt.Println("→ starting IPNI service")
	closeIPNI := bootstrap.StartIPNIService(t, ipniFindURL, ipniAnnounceURL)
	t.Cleanup(closeIPNI)
	fmt.Printf("✔ IPNI find and announce services running at %s and %s\n", ipniFindURL.String(), ipniAnnounceURL.String())

	fmt.Println("→ starting indexing service")
	closeIndexing := bootstrap.StartIndexingService(t, indexingID, indexingURL, ipniFindURL, ipniAnnounceURL, indexingNoCache)
	t.Cleanup(closeIndexing)
	fmt.Printf("✔ indexing service (%s) running at %s\n", indexingID.DID(), indexingURL.String())

	fmt.Println("→ starting storage node")
	closeStorage := bootstrap.StartStorageNode(t, storageID, storageURL, ipniAnnounceURL, indexingID, indexingURL, storageIndexingProof)
	t.Cleanup(closeStorage)
	fmt.Printf("✔ storage node (%s) running at %s\n", storageID.DID(), storageURL.String())

	fmt.Println("→ creating indexing service client")
	indexingClient, err := client.New(indexingID, indexingURL)
	require.NoError(t, err)
	fmt.Printf("✔ indexing service client created\n")

	fmt.Println("→ creating upload service")
	uploadService := upload.NewService(t, upload.Config{
		ID:             uploadID,
		StorageNodeID:  storageID,
		StorageNodeURL: storageURL,
		StorageProof:   uploadStorageProof,
	})
	fmt.Printf("✔ upload service (%s) created\n", uploadID.DID())

	return uploadService, indexingClient
}

func generateContent(t *testing.T, size int) (ipld.Link, multihash.Multihash, multihash.Multihash, []byte) {
	fmt.Println("→ generating content")
	root, rootDigest, digest, data := testutil.RandomCAR(t, size)
	fmt.Printf("✔ generation success\n")
	fmt.Printf("  root: %s (%s)\n", root.String(), digestutil.Format(rootDigest))
	fmt.Printf("  blob: %s\n", digestutil.Format(digest))
	return root, rootDigest, digest, data
}

func putBlob(t *testing.T, location url.URL, headers http.Header, data []byte) {
	fmt.Printf("→ performing http/put to %s\n", location.String())
	req, err := http.NewRequest("PUT", location.String(), bytes.NewReader(data))
	require.NoError(t, err)
	req.Header = headers

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	fmt.Println("✔ index http/put success")
}

func decodeLocationCommitmentCaveats(t *testing.T, claim delegation.Delegation) assert.LocationCaveats {
	fmt.Println("→ decoding location commitment")
	nb, rerr := assert.LocationCaveatsReader.Read(claim.Capabilities()[0].Nb())
	require.NoError(t, rerr)
	fmt.Printf("✔ decode success - %s @ %s\n", claim.Capabilities()[0].Can(), nb.Location[0].String())
	return nb
}

func fetchBlob(t *testing.T, location url.URL) ([]byte, multihash.Multihash) {
	fmt.Printf("→ fetching blob from %s\n", location.String())
	req, err := http.Get(location.String())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, req.StatusCode)
	data, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	digest, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	fmt.Println("✔ fetch success")
	return data, digest
}

func generateIndex(t *testing.T, content ipld.Link, carBytes []byte) (blobindex.ShardedDagIndexView, multihash.Multihash, ipld.Link, []byte) {
	fmt.Println("→ generating index")
	index, err := blobindex.FromShardArchives(content, [][]byte{carBytes})
	require.NoError(t, err)
	bytes, err := io.ReadAll(testutil.Must(index.Archive())(t))
	require.NoError(t, err)
	digest, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	require.NoError(t, err)
	link := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), digest)}
	fmt.Printf("✔ index created: %s (%s)\n", link.String(), digestutil.Format(digest))
	return index, digest, link, bytes
}

func CollectIndexes(t *testing.T, result types.QueryResult) []blobindex.ShardedDagIndexView {
	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(result.Blocks()))
	require.NoError(t, err)

	var indexes []blobindex.ShardedDagIndexView
	for _, link := range result.Indexes() {
		b, ok, err := br.Get(link)
		require.NoError(t, err)
		require.True(t, ok)

		index, err := blobindex.Extract(bytes.NewReader(b.Bytes()))
		require.NoError(t, err)
		indexes = append(indexes, index)
	}
	return indexes
}

func CollectClaims(t *testing.T, result types.QueryResult) []delegation.Delegation {
	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(result.Blocks()))
	require.NoError(t, err)

	var claims []delegation.Delegation
	for _, link := range result.Claims() {
		claim, err := delegation.NewDelegationView(link, br)
		require.NoError(t, err)
		claims = append(claims, claim)
	}
	return claims
}

func ContainsIndexClaim(t *testing.T, claims []delegation.Delegation, content ipld.Link, index ipld.Link) bool {
	return slices.ContainsFunc(claims, func(claim delegation.Delegation) bool {
		cap := claim.Capabilities()[0]
		if cap.Can() != assert.IndexAbility {
			return false
		}
		nb, err := assert.IndexCaveatsReader.Read(cap.Nb())
		require.NoError(t, err)
		return nb.Content == content && nb.Index == index
	})
}

func ContainsLocationCommitment(t *testing.T, claims []delegation.Delegation, content multihash.Multihash, space did.DID) bool {
	return slices.ContainsFunc(claims, func(claim delegation.Delegation) bool {
		cap := claim.Capabilities()[0]
		if cap.Can() != assert.LocationAbility {
			return false
		}
		nb, err := assert.LocationCaveatsReader.Read(cap.Nb())
		require.NoError(t, err)
		return bytes.Equal(nb.Content.Hash(), content) && nb.Space == space
	})
}

func publishIndexClaim(t *testing.T, indexingClient *client.Client, issuer principal.Signer, proof delegation.Proof, content ipld.Link, index ipld.Link) {
	fmt.Printf("→ performing assert/index with %s\n", index.String())
	err := indexingClient.PublishIndexClaim(context.Background(), issuer, assert.IndexCaveats{
		Content: content,
		Index:   index,
	}, delegation.WithProof(proof))
	require.NoError(t, err)
	fmt.Println("✔ assert/index success")
}

func QueryClaims(t *testing.T, indexingClient *client.Client, digest multihash.Multihash, space did.DID) types.QueryResult {
	if space == did.Undef {
		fmt.Printf("→ performing query for %s\n", digestutil.Format(digest))
	} else {
		fmt.Printf("→ performing query for %s, filtered by space %s\n", digestutil.Format(digest), space.String())
	}
	var match types.Match
	if space != did.Undef {
		match.Subject = append(match.Subject, space)
	}
	result, err := indexingClient.QueryClaims(context.Background(), types.Query{
		Hashes: []multihash.Multihash{digest},
		Match:  match,
	})
	require.NoError(t, err)
	fmt.Println("✔ query success")
	return result
}
