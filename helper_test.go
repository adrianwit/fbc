package fbc_test

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"os"
	"path/filepath"
	"testing"
)

func getTestConfig(t *testing.T) (*dsc.Config, error) {
	if !toolbox.FileExists(filepath.Join(os.Getenv("HOME"), ".secret", "fbc.json")) {
		t.Skip("skipping, not test credential not configured")
		return nil, nil
	}
	databaseURL := getEnvValue("testFireBaseDatabaseURL", "https://abstractdb-154a9.firebaseio.com")
	projectID := getEnvValue("testFireBaseProjectID", "abstractdb-154a9")
	storageBucket := getEnvValue("testFireBaseStorageBucket", "abstractdb-154a9.appspot.com")
	return dsc.NewConfigWithParameters("fbc", "", "abstractdb", map[string]interface{}{
		"databaseURL":   databaseURL,
		"projectID":     projectID,
		"storageBucket": storageBucket,
	})
}

func getEnvValue(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
