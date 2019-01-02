package fbc_test

import (
	_ "github.com/adrianwit/fbc"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestNewConnection(t *testing.T) {
	config, err := getTestConfig(t)
	if config == nil {
		return
	}
	if !assert.Nil(t, err) {
		return
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if !assert.Nil(t, err) {
		return
	}
	provider := manager.ConnectionProvider()
	_, err = provider.NewConnection()
	assert.Nil(t, err)
}
