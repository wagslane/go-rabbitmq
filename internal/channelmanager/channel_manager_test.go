package channelmanager

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/connectionmanager"
)

const enableDockerIntegrationTestsFlag = `ENABLE_DOCKER_INTEGRATION_TESTS`

// hostPort is distinct from the root-package integration tests' 5672 so the
// packages can run in parallel under `go test ./...`.
const hostPort = "5673"

type staticResolver struct{ url string }

func (r staticResolver) Resolve() ([]string, error) { return []string{r.url}, nil }

type testLogger struct{ t *testing.T }

func (l testLogger) Fatalf(format string, args ...interface{}) { l.t.Logf("FATAL: "+format, args...) }
func (l testLogger) Errorf(format string, args ...interface{}) { l.t.Logf("ERROR: "+format, args...) }
func (l testLogger) Warnf(format string, args ...interface{})  { l.t.Logf("WARN: "+format, args...) }
func (l testLogger) Infof(format string, args ...interface{})  { l.t.Logf("INFO: "+format, args...) }
func (l testLogger) Debugf(format string, args ...interface{}) { l.t.Logf("DEBUG: "+format, args...) }

func prepareDockerTest(t *testing.T) (connStr string) {
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
		return
	}

	out, err := exec.Command("docker", "run", "--rm", "--detach", "--publish="+hostPort+":5672", "--quiet", "--", "rabbitmq:4.1.1-alpine").Output()
	if err != nil {
		t.Fatalf("error launching rabbitmq in docker: %v", err)
	}
	t.Cleanup(func() {
		containerId := strings.TrimSpace(string(out))
		if err := exec.Command("docker", "rm", "--force", containerId).Run(); err != nil {
			t.Logf("failed to stop container %s: %v", containerId, err)
		}
	})
	return "amqp://guest:guest@localhost:" + hostPort + "/"
}

func waitForConnectionManager(t *testing.T, connStr string) *connectionmanager.ConnectionManager {
	deadline := time.Now().Add(30 * time.Second)
	for {
		connManager, err := connectionmanager.NewConnectionManager(staticResolver{connStr}, amqp.Config{}, testLogger{t}, time.Second, time.Second)
		if err == nil {
			return connManager
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for healthy amqp: %v", err)
		}
		time.Sleep(time.Second)
	}
}

// A channel that dies uncleanly sends its manager into reconnectLoop. If the
// manager is Closed while that loop is in flight, the loop must stop instead
// of installing a fresh channel that nothing will ever close (the channel
// leak that exhausts the connection's channel id space).
func TestCloseDuringReconnectStopsReconnectLoop(t *testing.T) {
	connStr := prepareDockerTest(t)
	connManager := waitForConnectionManager(t, connStr)
	defer connManager.Close()

	reconnectInterval := 500 * time.Millisecond
	chanManager, err := NewChannelManager(connManager, testLogger{t}, reconnectInterval, reconnectInterval)
	if err != nil {
		t.Fatalf("error creating channel manager: %v", err)
	}

	// Passive-declare of a queue that doesn't exist: the server answers 404
	// and closes the channel, which kicks off reconnectLoop.
	if _, err := chanManager.QueueDeclarePassiveSafe("no-such-queue", false, false, false, false, nil); err == nil {
		t.Fatal("expected passive declare of missing queue to fail")
	}

	// Close while reconnectLoop is sleeping its reconnectInterval.
	if err := chanManager.Close(); err != nil {
		t.Logf("close returned error (expected, channel already dead): %v", err)
	}

	// Give reconnectLoop ample time to complete an attempt.
	time.Sleep(4 * reconnectInterval)

	chanManager.channelMu.RLock()
	channel := chanManager.channel
	chanManager.channelMu.RUnlock()

	if !channel.IsClosed() {
		t.Fatal("channel manager installed a new open channel after Close: leaked channel")
	}
}
