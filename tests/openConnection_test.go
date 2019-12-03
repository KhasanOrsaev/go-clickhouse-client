package tests

import (
	clickhouse_client_git "git.fin-dev.ru/dmp/clickhouse_client.git"

	"io/ioutil"
	"testing"
)

func TestOpenConnection(t *testing.T) {
	client:= clickhouse_client_git.NewClient()
	f,err := ioutil.ReadFile("config_test.yaml")
	if err != nil {
		t.Error(err)
	}
	err = client.SetConfig(f)
	if err != nil {
		t.Error(err)
	}
	err = client.OpenConnection()
	if err != nil {
		t.Error(err)
	}
}
