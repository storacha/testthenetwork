package upload

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-capabilities/pkg/blob"
	bdm "github.com/storacha/go-capabilities/pkg/blob/datamodel"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	uhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/testthenetwork/internal/digestutil"
	"github.com/storacha/testthenetwork/internal/testutil"
	"github.com/stretchr/testify/require"
)

type Config struct {
	ID principal.Signer
	// StorageNodeID is the DID of the storage node.
	StorageNodeID ucan.Principal
	// StorageNodeURL is the URL of the storage node UCAN endpoint.
	StorageNodeURL url.URL
	// StorageProof is a delegation allowing the upload service to invoke
	// blob/allocate and blob/accept on the storage node.
	StorageProof delegation.Proof
}

// UploadService simulates actions taken by the upload service in response to
// client invocations.
type UploadService struct {
	cfg  Config
	conn client.Connection
}

// BlobAdd simulates a blob/add invocation from a client to the upload service.
// It sends a blob/allocate invocation to the storage node and returns the
// upload address if required (i.e. it may be nil if the storage node already
// has the blob).
func (s *UploadService) BlobAdd(t *testing.T, space did.DID, digest multihash.Multihash, size uint64) *blob.Address {
	fmt.Printf("→ performing blob/add with %s\n", digestutil.Format(digest))
	defer fmt.Println("✔ blob/add success")

	inv, err := blob.Allocate.Invoke(
		s.cfg.ID,
		s.cfg.StorageNodeID,
		s.cfg.StorageNodeID.DID().String(),
		blob.AllocateCaveats{
			Space: space,
			Blob: blob.Blob{
				Digest: digest,
				Size:   size,
			},
			Cause: testutil.RandomCID(t),
		},
		delegation.WithProof(s.cfg.StorageProof),
	)
	require.NoError(t, err)

	res, err := client.Execute([]invocation.Invocation{inv}, s.conn)
	require.NoError(t, err)

	reader, err := receipt.NewReceiptReaderFromTypes[bdm.AllocateOkModel, ipld.Node](bdm.AllocateOkType(), testutil.AnyType())
	require.NoError(t, err)

	rcptLink, ok := res.Get(inv.Link())
	require.True(t, ok)

	rcpt, err := reader.Read(rcptLink, res.Blocks())
	require.NoError(t, err)

	alloc, errNode := result.Unwrap(rcpt.Out())
	if errNode != nil {
		require.Nil(t, testutil.BindFailure(t, errNode))
	}
	if alloc.Address == nil {
		return nil
	}

	url, err := url.Parse(alloc.Address.Url)
	require.NoError(t, err)

	headers := http.Header{}
	for k, v := range alloc.Address.Headers.Values {
		headers.Set(k, v)
	}

	return &blob.Address{
		URL:     *url,
		Headers: headers,
		Expires: uint64(alloc.Address.Expires),
	}
}

// ConcludeHTTPPut simulates a ucan/conclude invocation for a http/put receipt
// from the client. It sends a blob/accept invocation to the storage node and
// returns the location commitment.
func (s *UploadService) ConcludeHTTPPut(t *testing.T, space did.DID, digest multihash.Multihash, size uint64, expires uint64) delegation.Delegation {
	fmt.Println("→ performing ucan/conclude for http/put")
	defer fmt.Println("✔ ucan/conclude success")

	inv, err := blob.Accept.Invoke(
		s.cfg.ID,
		s.cfg.StorageNodeID,
		s.cfg.StorageNodeID.DID().String(),
		blob.AcceptCaveats{
			Space: space,
			Blob: blob.Blob{
				Digest: digest,
				Size:   size,
			},
			Expires: expires,
			Put: blob.Promise{
				UcanAwait: blob.Await{
					Selector: ".out.ok",
					Link:     testutil.RandomCID(t),
				},
			},
		},
		delegation.WithProof(s.cfg.StorageProof),
	)
	require.NoError(t, err)

	res, err := client.Execute([]invocation.Invocation{inv}, s.conn)
	require.NoError(t, err)

	reader, err := receipt.NewReceiptReaderFromTypes[bdm.AcceptOkModel, ipld.Node](bdm.AcceptOkType(), testutil.AnyType())
	require.NoError(t, err)

	rcptLink, ok := res.Get(inv.Link())
	require.True(t, ok)

	rcpt, err := reader.Read(rcptLink, res.Blocks())
	require.NoError(t, err)

	acc, errNode := result.Unwrap(rcpt.Out())
	if errNode != nil {
		require.Nil(t, testutil.BindFailure(t, errNode))
	}

	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(res.Blocks()))
	require.NoError(t, err)

	claim, err := delegation.NewDelegationView(acc.Site, br)
	require.NoError(t, err)

	return claim
}

func NewService(t *testing.T, cfg Config) *UploadService {
	ch := uhttp.NewHTTPChannel(&cfg.StorageNodeURL)
	conn, err := client.NewConnection(cfg.StorageNodeID, ch)
	require.NoError(t, err)

	return &UploadService{cfg, conn}
}
