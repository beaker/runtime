package kubernetes

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/beaker/runtime/internal/test"
)

func TestLabelRegex(t *testing.T) {
	tests := map[string]struct {
		Value string
		Match bool
	}{
		"Empty":     {"", true},
		"OneChar":   {"a", true},
		"TwoChar":   {"ab", true},
		"Complex":   {"a-b.c_d", true},
		"BadPrefix": {"-no", false},
		"BadSuffix": {"no-", false},
		"BadChars":  {"a,b", false},
		"TooLong":   {"abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789", false},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.Match, labelRegex.Match([]byte(test.Value)))
		})
	}
}

const testKubernetesKey = "TEST_KUBERNETES"

func TestKubernetes(t *testing.T) {
	node, ok := os.LookupEnv(testKubernetesKey)
	if !ok {
		t.Skipf("Define %s=<node-id> to run Kubernetes tests.", testKubernetesKey)
	}
	if testing.Short() {
		t.Skipf("Skipped tests due to -short flag.")
	}

	rt, err := NewInClusterRuntime(context.Background(), "beaker-test", node)
	require.NoError(t, err)

	suite.Run(t, test.NewRuntimeSuite(rt))
}
