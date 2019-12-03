package clickhouse_client_git

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"net/http"
	"sync"
)

type DWHRequest struct {
	Request Request `json:"dwh"`
}
type Request struct {
	UserID        int             `json:"user_id,omitempty"`
	ApplicationID string          `json:"application_id,omitempty"`
	Type          string          `json:"type,omitempty"`
	ProcessID     string          `json:"process_id,omitempty"`
	Source        string          `json:"source,omitempty"`
	OfferID       string          `json:"offer_id,omitempty"`
	OfferStatus   string          `json:"offer_status,omitempty"`
	OfferSource   string          `json:"offer_source,omitempty"`
	OfferPayout   string          `json:"offer_payout,omitempty"`
	OfferLink     string          `json:"offer_link,omitempty"`
	DtCreate      string          `json:"dt_create,omitempty"`
	DtEvent       string          `json:"dt_event,omitempty"`
	DataJSON      *json.RawMessage `json:"data_json,omitempty"`
}

func (client *ClickHouseClient) WriteData(outChannel <-chan map[interface{}][]byte, confirmChannel chan<- interface{},
	crashChannel chan<- []byte, errChannel chan<- error) {
	ws := sync.WaitGroup{}
	for i := 0; i < client.Configuration.Workers; i++ {
		ws.Add(1)
		go func(outChannel <-chan map[interface{}][]byte, confirmChannel chan<- interface{}, crashChannel chan<- []byte, group *sync.WaitGroup) {
			defer group.Done()
			requests := make([]Request, 0, client.Configuration.Bulk)
			for d := range outChannel {
				r := DWHRequest{}
				// если достигнуто предельное значение в пачке, то пишем в базу
				if len(requests) == client.Configuration.Bulk {
					client.sendToDB(requests, crashChannel, errChannel)
					requests = make([]Request, 0, client.Configuration.Bulk)
				}
				for j, v := range d {
					fmt.Println(string(v))
					// данные из очереди, json декодировка
					err := json.Unmarshal(v, &r)
					if err != nil {
						confirmChannel <- j
						crashChannel <- v
						errChannel <- errors.Wrap(err, -1)
						continue
					}

					// validation
					if r.Request.UserID == 0 && r.Request.ApplicationID=="" {
						confirmChannel <- j
						crashChannel <- v
						errChannel <- errors.New("user_id and application_id is empty")
						continue
					}

					// enrichment

					requests = append(requests, r.Request)

					confirmChannel <- j
				}
			}
			// если по завершению цикла в пачке есть записи, то пишем в базу
			if len(requests) > 0 {
				client.sendToDB(requests, crashChannel, errChannel)
			}
		}(outChannel, confirmChannel, crashChannel, &ws)
	}
	ws.Wait()
}

// если ошибка записи то отправляем всю пачку в crash
func toCrashChannel(requests []Request, crashChannel chan<- []byte) {
	for request := range requests {
		r, _ := json.Marshal(request)
		crashChannel <- r
	}
}

func (client *ClickHouseClient) sendToDB(requests []Request, crashChannel chan<- []byte, errChannel chan<- error) {
	data,err := json.Marshal(requests)
	if err != nil {
		errChannel <- errors.Wrap(err ,-1)
		toCrashChannel(requests, crashChannel)
		return
	}
	body:= append([]byte(client.Configuration.Queries["insert"] + " format JSONEachRow "), data[1:len(string(data))-1]...)
	tr := &http.Transport{TLSClientConfig:&tls.Config{InsecureSkipVerify:true}}
	httpClient := http.Client{Transport:tr}
	request, err := http.NewRequest("POST", fmt.Sprintf(client.Configuration.Host, client.Configuration.User,
		client.Configuration.Password),	bytes.NewBuffer(body))
	if err!= nil {
		errChannel <- errors.Wrap(err ,-1)
		toCrashChannel(requests, crashChannel)
		return
	}
	response, err := httpClient.Do(request)
	if err!= nil {
		errChannel <- errors.Wrap(err ,-1)
		toCrashChannel(requests, crashChannel)
		return
	}
	if response.StatusCode!=200 {
		errChannel <- errors.New(response.Status)
		toCrashChannel(requests, crashChannel)
		return
	}
}
