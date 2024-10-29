package digestutil

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
)

func Format(digest multihash.Multihash) string {
	key, _ := multibase.Encode(multibase.Base58BTC, digest)
	return key
}

func ExtractDigest(link ipld.Link) multihash.Multihash {
	if cl, ok := link.(cidlink.Link); ok {
		return cl.Cid.Hash()
	}
	return cid.MustParse(link.String()).Hash()
}
