package sync

import (
	"context"
	"math/rand"
	"time"

	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/msg"
	wsclient "github.com/aurawing/auramq/ws/cli"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/subchen/go-trylock/v2"
	ytcrypto "github.com/yottachain/YTCrypto"
)

//Service sync service
type Service struct {
	client auramq.Client
	lock   trylock.TryLocker
}

//StartSync start syncing
func StartSync(subscriberBufferSize, pingWait, readWait, writeWait int, topic string, urls []string, callback func(msg *msg.Message), account, privateKey, clientID string) (map[int]*Service, error) {
	entry := log.WithFields(log.Fields{"function": "StartSync"})
	serviceMap := make(map[int]*Service)
	data := []byte(getRandomString(8))
	signature, err := ytcrypto.Sign(privateKey, data)
	if err != nil {
		entry.WithError(err).Error("generating signature")
		return nil, err
	}
	sigMsg := &pb.SignMessage{AccountName: account, Data: data, Signature: signature}
	crendData, err := proto.Marshal(sigMsg)
	if err != nil {
		entry.WithError(err).Error("encoding SignMessage")
		return nil, err
	}
	for i := range urls {
		serviceMap[i] = new(Service)
		serviceMap[i].lock = trylock.New()
		serviceMap[i].lock.Lock()
	}
	for i, url := range urls {
		snID := i
		wsurl := url
		go func() {
			for {
				svc := serviceMap[snID]
				cli, err := wsclient.Connect(wsurl, callback, &msg.AuthReq{Id: clientID, Credential: crendData}, []string{topic}, subscriberBufferSize, pingWait, readWait, writeWait)
				if err != nil {
					entry.WithError(err).Errorf("connecting to SN%d", snID)
					time.Sleep(time.Duration(3) * time.Second)
					continue
				}
				svc.client = cli
				svc.lock.Unlock()
				cli.Run()
				svc.lock.Lock()
				entry.WithError(err).Errorf("connect to SN%d successful", snID)
				time.Sleep(time.Duration(3) * time.Second)
			}
		}()
	}
	return serviceMap, nil
}

//Send one message to another client
func (s *Service) Send(to string, content []byte) bool {
	if ok := s.lock.RTryLock(context.Background()); !ok {
		return false
	}
	defer s.lock.RUnlock()
	return s.client.Send(to, content)
}

//Publish one message
func (s *Service) Publish(topic string, content []byte) bool {
	if ok := s.lock.RTryLock(context.Background()); !ok {
		return false
	}
	return s.client.Publish(topic, content)
}

func getRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}
