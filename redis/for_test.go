package redis_test

import (
	"os"
	"strconv"

	"github.com/alwitt/goutils/redis"
	"github.com/stretchr/testify/assert"
)

// getRedisConnectParamForTest helper function to get the REDIS connection parameters
// for unit testing
func getRedisConnectParamForTest(assert *assert.Assertions) redis.ConnectionConfig {
	serverHostStr := os.Getenv("UNITTEST_REDIS_HOST")
	assert.NotEmpty(serverHostStr)
	serverPortStr := os.Getenv("UNITTEST_REDIS_PORT")
	assert.NotEmpty(serverPortStr)
	serverDBStr := os.Getenv("UNITTEST_REDIS_DB")
	assert.NotEmpty(serverDBStr)

	serverPort, err := strconv.Atoi(serverPortStr)
	assert.Nil(err)

	serverDB, err := strconv.Atoi(serverDBStr)
	assert.Nil(err)

	return redis.ConnectionConfig{
		Host: serverHostStr, Port: uint16(serverPort), DBNumber: uint32(serverDB),
	}
}
