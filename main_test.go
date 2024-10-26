package main

import (
	"testing"

	"github.com/storacha/go-capabilities/pkg/assert"
	"github.com/storacha/go-capabilities/pkg/blob"
	"github.com/storacha/go-capabilities/pkg/claim"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/testthenetwork/internal/testutil"
	"github.com/storacha/testthenetwork/internal/upload"
)

func TestUpload(t *testing.T) {
	// Start IPNI ////////////////////////////////////////////////////////////////
	ipniFindURL := testutil.GenURL(t)
	ipniAnnounceURL := testutil.GenURL(t)

	testutil.StartIPNIService(t, ipniFindURL, ipniAnnounceURL)

	// Start Indexing Service ////////////////////////////////////////////////////
	indexingID := testutil.GenSigner(t)
	indexingURL := testutil.GenURL(t)

	testutil.StartIndexingService(t, indexingID, indexingURL, ipniFindURL, ipniAnnounceURL)

	// Start Storage Node ////////////////////////////////////////////////////////
	storageID := testutil.GenSigner(t)
	storageURL := testutil.GenURL(t)
	// proof storage node can invoke on indexing service
	storageIndexerProof := delegation.FromDelegation(
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
	testutil.StartStorageNode(t, storageID, storageURL, ipniAnnounceURL, indexingID, indexingURL, storageIndexerProof)

	// Start Upload Service //////////////////////////////////////////////////////
	uploadID := testutil.GenSigner(t)
	// proof upload service can invoke on storage node
	uploadStorageProof := delegation.FromDelegation(
		testutil.Must(
			delegation.Delegate(
				storageID,
				uploadID,
				[]ucan.Capability[ucan.NoCaveats]{
					ucan.NewCapability(
						blob.AllocateAbility,
						uploadID.DID().String(),
						ucan.NoCaveats{},
					),
					ucan.NewCapability(
						blob.AcceptAbility,
						uploadID.DID().String(),
						ucan.NoCaveats{},
					),
				},
			),
		)(t),
	)
	// proof upload service can invoke on indexing service
	uploadIndexerProof := delegation.FromDelegation(
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

	uploadService := upload.NewService(uploadID, uploadStorageProof, uploadIndexerProof)

	space := testutil.GenPrincipal(t).DID()
	digest, bytes := testutil.GenBytes(t, 256)

	uploadService.BlobAdd(space, digest, uint64(len(bytes)))
}
