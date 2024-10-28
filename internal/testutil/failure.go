package testutil

import (
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/stretchr/testify/require"
)

func AnyType() schema.Type {
	ts := new(schema.TypeSystem)
	ts.Init()
	ts.Accumulate(schema.SpawnAny("Any"))
	return ts.TypeByName("Any")
}

// BindFailure binds the IPLD node to a FailureModel if possible. This works
// around IPLD requiring data to match the schema exactly
func BindFailure(t testing.TB, n ipld.Node) fdm.FailureModel {
	t.Helper()
	require.Equal(t, n.Kind(), datamodel.Kind_Map)
	f := fdm.FailureModel{}

	nn, err := n.LookupByString("name")
	if err == nil {
		name, err := nn.AsString()
		require.NoError(t, err)
		f.Name = &name
	}

	mn, err := n.LookupByString("message")
	require.NoError(t, err)
	msg, err := mn.AsString()
	require.NoError(t, err)
	f.Message = msg

	sn, err := n.LookupByString("stack")
	if err == nil {
		stack, err := sn.AsString()
		require.NoError(t, err)
		f.Stack = &stack
	}

	return f
}
