package cri

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/beaker/runtime/internal/test"
)

const testCRIKey = "TEST_CRI_ADDRESS"

func TestCRI(t *testing.T) {
	address, ok := os.LookupEnv(testCRIKey)
	if !ok {
		t.Skipf("Define %s=<address> to run CRI tests.", testCRIKey)
	}
	if testing.Short() {
		t.Skipf("Skipped tests due to -short flag.")
	}

	rt, err := NewRuntime(context.Background(), address)
	require.NoError(t, err)

	suite.Run(t, test.NewRuntimeSuite(rt))
}
