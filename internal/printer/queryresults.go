package printer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/storacha/go-capabilities/pkg/assert"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/testthenetwork/internal/digestutil"
	"github.com/stretchr/testify/require"
)

func PrintQueryResults(t *testing.T, results types.QueryResult) {
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
