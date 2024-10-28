package testutil

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/alanshaw/storetheindex/config"
	"github.com/alanshaw/storetheindex/ingest"
	"github.com/alanshaw/storetheindex/registry"
	httpfind "github.com/alanshaw/storetheindex/server/find"
	httpingest "github.com/alanshaw/storetheindex/server/ingest"
	"github.com/ipfs/go-datastore"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/indexing-service/pkg/construct"
	idxsrv "github.com/storacha/indexing-service/pkg/server"
	"github.com/storacha/storage/pkg/server"
	"github.com/storacha/storage/pkg/service/storage"
	"github.com/storacha/storage/pkg/store/blobstore"
	"github.com/storacha/testthenetwork/internal/redis"
	"github.com/stretchr/testify/require"
)

func StartIPNIService(
	t *testing.T,
	announceURL url.URL,
	findURL url.URL,
) {
	indexerCore := engine.New(memory.New())

	reg, err := registry.New(
		context.Background(),
		config.NewDiscovery(),
		datastore.NewMapDatastore(),
	)
	require.NoError(t, err)

	p2pHost, err := libp2p.New()
	require.NoError(t, err)

	ingConfig := config.NewIngest()
	ingConfig.PubSubTopic = "/storacha/indexer/ingest/testnet"
	ing, err := ingest.NewIngester(
		ingConfig,
		p2pHost,
		indexerCore,
		reg,
		datastore.NewMapDatastore(),
		datastore.NewMapDatastore(),
	)
	require.NoError(t, err)

	announceAddr := fmt.Sprintf("%s:%s", announceURL.Hostname(), announceURL.Port())
	ingSvr, err := httpingest.New(announceAddr, indexerCore, ing, reg)
	require.NoError(t, err)

	go func() {
		err = ingSvr.Start()
	}()

	findAddr := fmt.Sprintf("%s:%s", findURL.Hostname(), findURL.Port())
	findSvr, err := httpfind.New(findAddr, indexerCore, reg)

	go func() {
		err = findSvr.Start()
	}()

	time.Sleep(time.Millisecond * 100)
	require.NoError(t, err)
}

func StartIndexingService(
	t *testing.T,
	id principal.Signer,
	publicURL url.URL,
	indexerURL url.URL,
	directAnnounceURL url.URL,
) {
	privKey, err := crypto.UnmarshalEd25519PrivateKey(id.Raw())
	require.NoError(t, err)

	publisherListenURL := RandomLocalURL(t)
	announceAddr, err := maurl.FromURL(&publisherListenURL)
	require.NoError(t, err)

	cfg := construct.ServiceConfig{
		PrivateKey:                  privKey,
		PublicURL:                   []string{publicURL.String()},
		IndexerURL:                  indexerURL.String(),
		PublisherDirectAnnounceURLs: []string{directAnnounceURL.String()},
		PublisherListenAddr:         fmt.Sprintf("%s:%s", publisherListenURL.Hostname(), publisherListenURL.Port()),
		PublisherAnnounceAddrs:      []string{announceAddr.String()},
	}
	indexer, err := construct.Construct(
		cfg,
		construct.WithStartIPNIServer(true),
		construct.WithDatastore(datastore.NewMapDatastore()),
		construct.WithProvidersClient(redis.NewMapRedis()),
		construct.WithClaimsClient(redis.NewMapRedis()),
		construct.WithIndexesClient(redis.NewMapRedis()),
	)
	require.NoError(t, err)

	err = indexer.Startup(context.Background())
	require.NoError(t, err)

	go func() {
		addr := fmt.Sprintf("%s:%s", publicURL.Hostname(), publicURL.Port())
		err = idxsrv.ListenAndServe(addr, indexer, idxsrv.WithIdentity(id))
	}()

	time.Sleep(time.Millisecond * 100)
	require.NoError(t, err)
}

func StartStorageNode(
	t *testing.T,
	id principal.Signer,
	publicURL url.URL,
	announceURL url.URL,
	indexingServiceDID ucan.Principal,
	indexingServiceURL url.URL,
	indexingServiceProof delegation.Proof,
) {
	svc, err := storage.New(
		storage.WithIdentity(id),
		storage.WithBlobstore(blobstore.NewMapBlobstore()),
		storage.WithAllocationDatastore(datastore.NewMapDatastore()),
		storage.WithClaimDatastore(datastore.NewMapDatastore()),
		storage.WithPublisherDatastore(datastore.NewMapDatastore()),
		storage.WithPublicURL(publicURL),
		storage.WithPublisherDirectAnnounce(announceURL),
		storage.WithPublisherIndexingServiceConfig(indexingServiceDID, *indexingServiceURL.JoinPath("claims")),
		storage.WithPublisherIndexingServiceProof(indexingServiceProof),
	)
	require.NoError(t, err)

	go func() {
		addr := fmt.Sprintf("%s:%s", publicURL.Hostname(), publicURL.Port())
		err = server.ListenAndServe(addr, svc)
	}()

	time.Sleep(time.Millisecond * 100)
	require.NoError(t, err)
}
