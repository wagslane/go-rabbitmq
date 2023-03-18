package integration_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wagslane/go-rabbitmq"
)

func waitForConnections(require *require.Assertions, expected int) {
	maxWait := 30
	for i := 0; i < maxWait; i++ {
		req, err := http.NewRequest(http.MethodGet, mgmtUrl+"/api/connections", http.NoBody)
		require.NoError(err)

		req.SetBasicAuth(rabbitmqUser, rabbitmqPass)

		res, err := http.DefaultClient.Do(req)
		require.NoError(err)

		defer res.Body.Close()

		require.Equal(200, res.StatusCode)
		resBody, err := io.ReadAll(res.Body)
		require.NoError(err)

		connections := []map[string]interface{}{}
		require.NoError(json.Unmarshal(resBody, &connections))

		if len(connections) == expected {
			return
		}
		time.Sleep(1 * time.Second)
	}

	require.Fail("waitForConnections timed out")
}

func terminateConnections(require *require.Assertions) {
	req, err := http.NewRequest(http.MethodGet, mgmtUrl+"/api/connections", http.NoBody)
	require.NoError(err)

	req.SetBasicAuth(rabbitmqUser, rabbitmqPass)

	res, err := http.DefaultClient.Do(req)
	require.NoError(err)

	defer res.Body.Close()

	require.Equal(200, res.StatusCode)
	resBody, err := io.ReadAll(res.Body)
	require.NoError(err)

	connections := []map[string]interface{}{}
	require.NoError(json.Unmarshal(resBody, &connections))

	require.Len(connections, 2)

	for _, connection := range connections {
		name := connection["name"].(string)
		fmt.Println("connection", name)

		req, err := http.NewRequest(http.MethodDelete, mgmtUrl+"/api/connections/"+name, http.NoBody)
		require.NoError(err)
		req.SetBasicAuth(rabbitmqUser, rabbitmqPass)

		res, err := http.DefaultClient.Do(req)
		require.NoError(err)
		require.Equal(204, res.StatusCode)
	}
}

func publishTestMessage(require *require.Assertions, publisher *rabbitmq.Publisher) TestMessage {
	msg := TestMessage{Text: "Test"}

	msgBytes, err := json.Marshal(msg)
	require.NoError(err)

	require.NoError(publisher.Publish(msgBytes, []string{"test"}))

	return msg
}
