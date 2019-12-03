package clickhouse_client_git

import (
	"crypto/tls"
	"fmt"
	"git.fin-dev.ru/dmp/clickhouse_client.git/config"
	"github.com/go-errors/errors"
	"io/ioutil"
	"net/http"
)

type ClickHouseClient struct {
	Configuration *config.Configuration
}

var err error

func NewClient() *ClickHouseClient {
	return &ClickHouseClient{}
}

func (client *ClickHouseClient) SetConfig(f []byte) error {
	err = config.InitConfig(f)
	client.Configuration = config.GetConfig()
	return err
}

func (client *ClickHouseClient) OpenConnection() error {
	// disable certificate checking 
	tr := &http.Transport{TLSClientConfig:&tls.Config{InsecureSkipVerify:true}}
	httpClient := http.Client{Transport:tr}
	request, err := http.NewRequest("GET", fmt.Sprintf(client.Configuration.Host, client.Configuration.User, client.Configuration.Password),nil)
	if err!= nil {
		return errors.Wrap(err ,-1)
	}
	response, err := httpClient.Do(request)
	if err!= nil {
		return errors.Wrap(err ,-1)
	}
	b, err := ioutil.ReadAll(response.Body)
	if err!= nil {
		return errors.Wrap(err ,-1)
	}
	if string(b) != "Ok.\n" {
		return errors.New("Error on open connection")
	}
	return nil
}

func (client *ClickHouseClient) CloseConnection() error {
	return nil
}
