package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDB_panics_for_nonretryable_configuration_errors(t *testing.T) {
	t.Run("empty targets", func(t *testing.T) {
		require.PanicsWithError(t, "empty targets", func() {
			_, _ = newDB(nil, mysqlDriver, "root", "", "test", "")
		})
	})

	t.Run("unknown driver", func(t *testing.T) {
		require.PanicsWithError(t, `unknown driver: "invalid"`, func() {
			_, _ = newDB([]string{"127.0.0.1:4000"}, "invalid", "root", "", "test", "")
		})
	})

	t.Run("postgresql TLS", func(t *testing.T) {
		previousSSLCA := sslCA
		t.Cleanup(func() {
			sslCA = previousSSLCA
		})
		sslCA = "ca.pem"

		require.PanicsWithValue(t, "postgresql driver doesn't support TLS yet", func() {
			_, _ = newDB([]string{"127.0.0.1:5432"}, pgDriver, "root", "", "test", "")
		})
	})

	t.Run("incomplete MySQL key pair", func(t *testing.T) {
		previousSSLCA, previousSSLCert, previousSSLKey := sslCA, sslCert, sslKey
		t.Cleanup(func() {
			sslCA, sslCert, sslKey = previousSSLCA, previousSSLCert, previousSSLKey
		})
		sslCA = ""
		sslCert = "cert.pem"
		sslKey = ""

		require.PanicsWithValue(t, "incomplete key pair configuration", func() {
			_, _ = newDB([]string{"127.0.0.1:4000"}, mysqlDriver, "root", "", "test", "")
		})
	})
}
