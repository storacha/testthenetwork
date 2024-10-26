package upload

import (
	"net/http"
	"net/url"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
)

type UploadService struct {
	id         principal.Signer
	storagePrf delegation.Proof
	indexerPrf delegation.Proof
}

func (s *UploadService) BlobAdd(space did.DID, digest multihash.Multihash, size uint64) (url.URL, http.Header, error) {
	panic("not implemented")
}

func NewService(id principal.Signer, storagePrf delegation.Proof, indexerPrf delegation.Proof) *UploadService {
	return &UploadService{id, storagePrf, indexerPrf}
}
