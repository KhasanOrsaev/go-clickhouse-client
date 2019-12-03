package tests

import (
	"fmt"
	clickhouse_client_git "git.fin-dev.ru/dmp/clickhouse_client.git"
	"io/ioutil"
	"sync"
	"testing"
)

func TestWriteData(t *testing.T) {
	// канал ошибок
	errChannel := make(chan error, 10)
	outChannel := make(chan map[interface{}][]byte, 10)
	confirmChannel := make(chan interface{})
	crashChannel := make(chan []byte)
	f, err := ioutil.ReadFile("config_test.yaml")
	if err != nil {
		t.Fatal(err)
	}
	client := clickhouse_client_git.NewClient()
	err = client.SetConfig(f)
	if err != nil {
		t.Fatal(err)
	}
	err = client.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseConnection()
	data2 := []byte(`{"dwh":{"user_id" : 122223,"type" : "cs_mailgun_delivered","source" : "333333"}}`)
	go func() {
		for i := 0; i < 1; i++ {
			outChannel <- map[interface{}][]byte{2: data2}
		}
		close(outChannel)
	}()
	ws := sync.WaitGroup{}
	ws.Add(4)
	go func() {
		client.WriteData(outChannel, confirmChannel, crashChannel, errChannel)
		close(confirmChannel)
		close(crashChannel)
		close(errChannel)
		ws.Done()
	}()
	go func() {
		for b := range errChannel {
			t.Error("error:", b.Error())
		}
		ws.Done()
	}()

	go func() {
		for b := range crashChannel {
			fmt.Println("crash:", string(b))
		}
		ws.Done()
	}()
	go func() {
		for b := range confirmChannel {
			fmt.Println("confirm:", b)
		}
		ws.Done()
	}()
	ws.Wait()
}