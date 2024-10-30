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
		storageID, indexingID, uploadID, aliceID := generateIdentities(t)
		ipniFindURL, ipniAnnounceURL, storageURL, indexingURL := generateURLs(t)
		storageIndexingProof, uploadStorageProof, aliceIndexingProof := generateProofs(t, storageID, indexingID, uploadID, aliceID)
		uploadService, indexingClient := startServices(t, ipniFindURL, ipniAnnounceURL, storageID, storageURL, storageIndexingProof, indexingID, indexingURL, false, uploadID, uploadStorageProof)

		space := testutil.RandomPrincipal(t).DID()
		root, rootDigest, digest, data := generateContent(t, 256)

		address := uploadService.BlobAdd(t, space, digest, uint64(len(data)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, data)
		}
		claim := uploadService.ConcludeHTTPPut(t, space, digest, uint64(len(data)), address.Expires)

		nb := decodeLocationCommitmentCaveats(t, claim)

		blobBytes, blobDigest := fetchBlob(t, nb.Location[0])
		require.Equal(t, digest, blobDigest)

		_, indexDigest, indexLink, indexData := generateIndex(t, root, blobBytes)

		address = uploadService.BlobAdd(t, space, indexDigest, uint64(len(indexData)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, indexData)
		}
		uploadService.ConcludeHTTPPut(t, space, indexDigest, uint64(len(indexData)), address.Expires)

		publishIndexClaim(t, indexingClient, aliceID, aliceIndexingProof, root, indexLink)

		result := queryClaims(t, indexingClient, rootDigest, did.Undef)
		printer.PrintQueryResults(t, result)

		indexes := collectIndexes(t, result)
		require.Len(t, indexes, 1)
		require.Equal(t, indexLink, result.Indexes()[0]) // should be the index we generated

		claims := collectClaims(t, result)
		requireContainsIndexClaim(t, claims, root, indexLink)            // find an index claim for our root
		requireContainsLocationCommitment(t, claims, indexDigest, space) // find a location commitment for the index
		requireContainsLocationCommitment(t, claims, blobDigest, space)  // find a location commitment for the shard
	})

	t.Run("round trip (no cache)", func(t *testing.T) {
		storageID, indexingID, uploadID, aliceID := generateIdentities(t)
		ipniFindURL, ipniAnnounceURL, storageURL, indexingURL := generateURLs(t)
		storageIndexingProof, uploadStorageProof, aliceIndexingProof := generateProofs(t, storageID, indexingID, uploadID, aliceID)
		uploadService, indexingClient := startServices(t, ipniFindURL, ipniAnnounceURL, storageID, storageURL, storageIndexingProof, indexingID, indexingURL, true, uploadID, uploadStorageProof)

		space := testutil.RandomPrincipal(t).DID()
		root, rootDigest, digest, data := generateContent(t, 256)

		address := uploadService.BlobAdd(t, space, digest, uint64(len(data)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, data)
		}
		claim := uploadService.ConcludeHTTPPut(t, space, digest, uint64(len(data)), address.Expires)

		nb := decodeLocationCommitmentCaveats(t, claim)

		blobBytes, blobDigest := fetchBlob(t, nb.Location[0])
		require.Equal(t, digest, blobDigest)

		_, indexDigest, indexLink, indexData := generateIndex(t, root, blobBytes)

		address = uploadService.BlobAdd(t, space, indexDigest, uint64(len(indexData)))
		if address != nil {
			putBlob(t, address.URL, address.Headers, indexData)
		}
		uploadService.ConcludeHTTPPut(t, space, indexDigest, uint64(len(indexData)), address.Expires)

		publishIndexClaim(t, indexingClient, aliceID, aliceIndexingProof, root, indexLink)

		var result types.QueryResult
		for i := 0; i < 5; i++ {
			result = queryClaims(t, indexingClient, rootDigest, did.Undef)
			if len(result.Claims()) > 0 || len(result.Indexes()) > 0 {
				break
			}
			// no local cache so we have to wait for IPNI to crawl to the head
			fmt.Printf("→ waiting for IPNI sync %d/5\n", i+1)
			time.Sleep(time.Second)
		}
		printer.PrintQueryResults(t, result)

		indexes := collectIndexes(t, result)
		require.Len(t, indexes, 1)
		require.Equal(t, indexLink, result.Indexes()[0]) // should be the index we generated

		claims := collectClaims(t, result)
		requireContainsIndexClaim(t, claims, root, indexLink)            // find an index claim for our root
		requireContainsLocationCommitment(t, claims, indexDigest, space) // find a location commitment for the index
		requireContainsLocationCommitment(t, claims, blobDigest, space)  // find a location commitment for the shard
	})
}
