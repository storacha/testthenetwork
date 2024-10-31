package bootstrap

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/alanshaw/storetheindex/config"
	"github.com/alanshaw/storetheindex/ingest"
	"github.com/alanshaw/storetheindex/registry"
	httpfind "github.com/alanshaw/storetheindex/server/find"
	httpingest "github.com/alanshaw/storetheindex/server/ingest"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
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
	rsync "github.com/storacha/testthenetwork/internal/redis/sync"
	"github.com/storacha/testthenetwork/internal/testutil"
	"github.com/stretchr/testify/require"
)

func StartIPNIService(
	t *testing.T,
	findURL url.URL,
	announceURL url.URL,
) func() {
	indexerCore := engine.New(memory.New())

	reg, err := registry.New(
		context.Background(),
		config.NewDiscovery(),
		dssync.MutexWrap(datastore.NewMapDatastore()),
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
		dssync.MutexWrap(datastore.NewMapDatastore()),
		dssync.MutexWrap(datastore.NewMapDatastore()),
	)
	require.NoError(t, err)

	announceAddr := fmt.Sprintf("%s:%s", announceURL.Hostname(), announceURL.Port())
	ingSvr, err := httpingest.New(announceAddr, indexerCore, ing, reg)
	require.NoError(t, err)

	var ingStartErr error
	go func() {
		ingStartErr = ingSvr.Start()
	}()

	findAddr := fmt.Sprintf("%s:%s", findURL.Hostname(), findURL.Port())
	findSvr, err := httpfind.New(findAddr, indexerCore, reg)
	require.NoError(t, err)

	var findStartErr error
	go func() {
		findStartErr = findSvr.Start()
	}()

	time.Sleep(time.Millisecond * 100)
	require.NoError(t, ingStartErr)
	require.NoError(t, findStartErr)

	return func() {
		ingSvr.Close()
		ing.Close()
		findSvr.Close()
		reg.Close()
		indexerCore.Close()
		p2pHost.Close()
	}
}

func StartIndexingService(
	t *testing.T,
	id principal.Signer,
	publicURL url.URL,
	indexerURL url.URL,
	directAnnounceURL url.URL,
	noCache bool,
) func() {
	privKey, err := crypto.UnmarshalEd25519PrivateKey(id.Raw())
	require.NoError(t, err)

	publisherListenURL := testutil.RandomLocalURL(t)
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

	var indexer construct.Service
	if noCache {
		indexer, err = construct.Construct(
			cfg,
			construct.WithStartIPNIServer(true),
			construct.WithDatastore(dssync.MutexWrap(datastore.NewMapDatastore())),
			construct.WithProvidersClient(redis.NewBlackholeRedis()),
			construct.WithClaimsClient(redis.NewBlackholeRedis()),
			construct.WithIndexesClient(redis.NewBlackholeRedis()),
		)
	} else {
		indexer, err = construct.Construct(
			cfg,
			construct.WithStartIPNIServer(true),
			construct.WithDatastore(dssync.MutexWrap(datastore.NewMapDatastore())),
			construct.WithProvidersClient(rsync.MutexWrap(redis.NewMapRedis())),
			construct.WithClaimsClient(rsync.MutexWrap(redis.NewMapRedis())),
			construct.WithIndexesClient(rsync.MutexWrap(redis.NewMapRedis())),
		)
	}
	require.NoError(t, err)

	err = indexer.Startup(context.Background())
	require.NoError(t, err)

	go func() {
		addr := fmt.Sprintf("%s:%s", publicURL.Hostname(), publicURL.Port())
		err = idxsrv.ListenAndServe(addr, indexer, idxsrv.WithIdentity(id))
	}()

	time.Sleep(time.Millisecond * 100)
	require.NoError(t, err)

	return func() {
		indexer.Shutdown(context.Background())
	}
}

func StartStorageNode(
	t *testing.T,
	id principal.Signer,
	publicURL url.URL,
	announceURL url.URL,
	indexingServiceDID ucan.Principal,
	indexingServiceURL url.URL,
	indexingServiceProof delegation.Proof,
) func() {
	svc, err := storage.New(
		storage.WithIdentity(id),
		storage.WithBlobstore(blobstore.NewMapBlobstore()),
		storage.WithAllocationDatastore(dssync.MutexWrap(datastore.NewMapDatastore())),
		storage.WithClaimDatastore(dssync.MutexWrap(datastore.NewMapDatastore())),
		storage.WithPublisherDatastore(dssync.MutexWrap(datastore.NewMapDatastore())),
		storage.WithPublicURL(publicURL),
		storage.WithPublisherDirectAnnounce(announceURL),
		storage.WithPublisherIndexingServiceConfig(indexingServiceDID, *indexingServiceURL.JoinPath("claims")),
		storage.WithPublisherIndexingServiceProof(indexingServiceProof),
	)
	require.NoError(t, err)

	srvMux, err := server.NewServer(svc)
	require.NoError(t, err)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf("%s:%s", publicURL.Hostname(), publicURL.Port()),
		Handler: srvMux,
	}

	go func() {
		err = httpServer.ListenAndServe()
	}()

	time.Sleep(time.Millisecond * 100)
	require.NoError(t, err)

	return func() {
		httpServer.Close()
	}
}
