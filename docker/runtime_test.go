package docker

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/beaker/runtime/internal/test"
)

const testDockerKey = "TEST_DOCKER"

func TestDocker(t *testing.T) {
	if _, ok := os.LookupEnv(testDockerKey); !ok {
		t.Skipf("Define %s to run Docker tests.", testDockerKey)
	}
	if testing.Short() {
		t.Skipf("Skipped tests due to -short flag.")
	}

	rt, err := NewRuntime()
	require.NoError(t, err)

	suite.Run(t, test.NewRuntimeSuite(rt))
}
