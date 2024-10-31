package main

import (
	"fmt"
	"testing"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/testthenetwork/internal/printer"
	"github.com/storacha/testthenetwork/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestTheNetwork(t *testing.T) {
	logging.SetLogLevel("*", "warn")

	t.Run("round trip", func(t *testing.T) {
		storageID, indexingID, uploadID, aliceID, bobID := generateIdentities(t)
		ipniFindURL, ipniAnnounceURL, storageURL, indexingURL := generateURLs(t)
		storageIndexingProof, uploadStorageProof, aliceIndexingProof, _ := generateProofs(t, storageID, indexingID, uploadID, aliceID, bobID)
		uploadService, indexingClient := startServices(t, ipniFindURL, ipniAnnounceURL, storageID, storageURL, storageIndexingProof, indexingID, indexingURL, false, uploadID, uploadStorageProof)

		space := testutil.RandomPrincipal(t).DID()
		root, rootDigest, digest, data := generateContent(t, 256)

		address := uploadService.BlobAdd(t, space, digest, uint64(len(data)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, data)
		}
		claim := uploadService.ConcludeHTTPPut(t, space, digest, uint64(len(data)))

		nb := decodeLocationCommitmentCaveats(t, claim)

		blobBytes, blobDigest := fetchBlob(t, nb.Location[0])
		require.Equal(t, digest, blobDigest)

		_, indexDigest, indexLink, indexData := generateIndex(t, root, blobBytes)

		address = uploadService.BlobAdd(t, space, indexDigest, uint64(len(indexData)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, indexData)
		}
		uploadService.ConcludeHTTPPut(t, space, indexDigest, uint64(len(indexData)))

		publishIndexClaim(t, indexingClient, aliceID, aliceIndexingProof, root, indexLink)

		result := QueryClaims(t, indexingClient, rootDigest, did.Undef)
		printer.PrintQueryResults(t, result)

		indexes := CollectIndexes(t, result)
		require.Len(t, indexes, 1)
		require.Equal(t, indexLink, result.Indexes()[0]) // should be the index we generated

		claims := CollectClaims(t, result)
		require.True(t, ContainsIndexClaim(t, claims, root, indexLink))            // find an index claim for our root
		require.True(t, ContainsLocationCommitment(t, claims, indexDigest, space)) // find a location commitment for the index
		require.True(t, ContainsLocationCommitment(t, claims, blobDigest, space))  // find a location commitment for the shard
	})

	t.Run("round trip (no cache)", func(t *testing.T) {
		storageID, indexingID, uploadID, aliceID, bobID := generateIdentities(t)
		ipniFindURL, ipniAnnounceURL, storageURL, indexingURL := generateURLs(t)
		storageIndexingProof, uploadStorageProof, aliceIndexingProof, _ := generateProofs(t, storageID, indexingID, uploadID, aliceID, bobID)
		uploadService, indexingClient := startServices(t, ipniFindURL, ipniAnnounceURL, storageID, storageURL, storageIndexingProof, indexingID, indexingURL, true, uploadID, uploadStorageProof)

		space := testutil.RandomPrincipal(t).DID()
		root, rootDigest, digest, data := generateContent(t, 256)

		address := uploadService.BlobAdd(t, space, digest, uint64(len(data)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, data)
		}
		claim := uploadService.ConcludeHTTPPut(t, space, digest, uint64(len(data)))

		nb := decodeLocationCommitmentCaveats(t, claim)

		blobBytes, blobDigest := fetchBlob(t, nb.Location[0])
		require.Equal(t, digest, blobDigest)

		_, indexDigest, indexLink, indexData := generateIndex(t, root, blobBytes)

		address = uploadService.BlobAdd(t, space, indexDigest, uint64(len(indexData)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, indexData)
		}
		uploadService.ConcludeHTTPPut(t, space, indexDigest, uint64(len(indexData)))

		publishIndexClaim(t, indexingClient, aliceID, aliceIndexingProof, root, indexLink)

		var result types.QueryResult
		for i := 0; i < 5; i++ {
			result = QueryClaims(t, indexingClient, rootDigest, did.Undef)
			if len(result.Claims()) > 0 || len(result.Indexes()) > 0 {
				break
			}
			// no local cache so we have to wait for IPNI to crawl to the head
			fmt.Printf("→ waiting for IPNI sync %d/5\n", i+1)
			time.Sleep(time.Second)
		}
		printer.PrintQueryResults(t, result)

		indexes := CollectIndexes(t, result)
		require.Len(t, indexes, 1)
		require.Equal(t, indexLink, result.Indexes()[0]) // should be the index we generated

		claims := CollectClaims(t, result)
		require.True(t, ContainsIndexClaim(t, claims, root, indexLink))            // find an index claim for our root
		require.True(t, ContainsLocationCommitment(t, claims, indexDigest, space)) // find a location commitment for the index
		require.True(t, ContainsLocationCommitment(t, claims, blobDigest, space))  // find a location commitment for the shard
	})

	t.Run("filter by space", func(t *testing.T) {
		storageID, indexingID, uploadID, aliceID, bobID := generateIdentities(t)
		ipniFindURL, ipniAnnounceURL, storageURL, indexingURL := generateURLs(t)
		storageIndexingProof, uploadStorageProof, aliceIndexingProof, bobIndexingProof := generateProofs(t, storageID, indexingID, uploadID, aliceID, bobID)
		uploadService, indexingClient := startServices(t, ipniFindURL, ipniAnnounceURL, storageID, storageURL, storageIndexingProof, indexingID, indexingURL, false, uploadID, uploadStorageProof)

		aliceSpace := testutil.RandomPrincipal(t).DID()
		root, rootDigest, digest, data := generateContent(t, 256)

		address := uploadService.BlobAdd(t, aliceSpace, digest, uint64(len(data)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, data)
		}
		uploadService.ConcludeHTTPPut(t, aliceSpace, digest, uint64(len(data)))

		_, indexDigest, indexLink, indexData := generateIndex(t, root, data)

		address = uploadService.BlobAdd(t, aliceSpace, indexDigest, uint64(len(indexData)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, indexData)
		}
		uploadService.ConcludeHTTPPut(t, aliceSpace, indexDigest, uint64(len(indexData)))

		publishIndexClaim(t, indexingClient, aliceID, aliceIndexingProof, root, indexLink)

		// bob will attempt to upload the same blob
		bobSpace := testutil.RandomPrincipal(t).DID()

		address = uploadService.BlobAdd(t, bobSpace, digest, uint64(len(data)))
		require.Nil(t, address) // address should be nil since it is already uploaded
		uploadService.ConcludeHTTPPut(t, bobSpace, digest, uint64(len(data)))

		address = uploadService.BlobAdd(t, bobSpace, indexDigest, uint64(len(indexData)))
		require.Nil(t, address) // address should be nil since it is already uploaded
		uploadService.ConcludeHTTPPut(t, bobSpace, indexDigest, uint64(len(indexData)))

		publishIndexClaim(t, indexingClient, bobID, bobIndexingProof, root, indexLink)

		result := QueryClaims(t, indexingClient, rootDigest, bobSpace)
		printer.PrintQueryResults(t, result)

		indexes := CollectIndexes(t, result)
		require.Len(t, indexes, 1)
		require.Equal(t, indexLink, result.Indexes()[0]) // should be the index we generated

		claims := CollectClaims(t, result)
		require.True(t, ContainsIndexClaim(t, claims, root, indexLink))               // find an index claim for our root
		require.True(t, ContainsLocationCommitment(t, claims, indexDigest, bobSpace)) // find a location commitment for the index
		require.True(t, ContainsLocationCommitment(t, claims, digest, bobSpace))      // find a location commitment for the shard
		require.False(t, ContainsLocationCommitment(t, claims, indexDigest, aliceSpace))
		require.False(t, ContainsLocationCommitment(t, claims, digest, aliceSpace))
	})
}
