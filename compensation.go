package ytrebuilder

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

//ShardRebuildMeta metadata of shards rebuilt
type ShardRebuildMeta struct {
	ID          int64 `json:"_id"`
	VFI         int64 `json:"VFI"`
	DestMinerID int32 `json:"nid"`
	SrcMinerID  int32 `json:"sid"`
}

//ShardsRebuild struct
type ShardsRebuild struct {
	ShardsRebuild []*ShardRebuildMeta
	More          bool
	Next          int64
}

//CPSRecord struct
type CPSRecord struct {
	ID        int32 `bson:"_id"`
	Start     int64 `bson:"start"`
	Timestamp int64 `bson:"timestamp"`
}

//GetRebuildShards find rebuilt shards
func GetRebuildShards(httpCli *http.Client, url string, from int64, count int, skipTime int64) (*ShardsRebuild, error) {
	entry := log.WithFields(log.Fields{Function: "GetRebuildShards"})
	count++
	fullURL := fmt.Sprintf("%s/sync/getShardRebuildMetas?start=%d&count=%d", url, from, count)
	entry.Debugf("fetching rebuilt shards by URL: %s", fullURL)

	request, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		entry.WithError(err).Errorf("create request failed: %s", fullURL)
		return nil, err
	}
	request.Header.Add("Accept-Encoding", "gzip")
	resp, err := httpCli.Do(request)
	if err != nil {
		entry.WithError(err).Errorf("get rebuilt shards meta failed: %s", fullURL)
		return nil, err
	}
	defer resp.Body.Close()
	reader := io.Reader(resp.Body)
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gbuf, err := gzip.NewReader(reader)
		if err != nil {
			entry.WithError(err).Errorf("decompress response body: %s", fullURL)
			return nil, err
		}
		reader = io.Reader(gbuf)
		defer gbuf.Close()
	}
	response := make([]*ShardRebuildMeta, 0)
	err = json.NewDecoder(reader).Decode(&response)
	if err != nil {
		entry.WithError(err).Errorf("decode rebuilt shards metadata failed: %s", fullURL)
		return nil, err
	}
	skipByte32 := Int32ToBytes(int32(skipTime))
	padding := []byte{0x00, 0x00, 0x00, 0x00}
	skipTime64 := BytesToInt64(append(skipByte32, padding...))
	i := 0
	for _, item := range response {
		if item.ID > skipTime64 {
			break
		}
		i++
	}
	response = response[0:i]
	next := int64(0)
	entry.Debugf("fetched %d shards rebuilt", len(response))
	if len(response) == count {
		next = response[count-1].ID
		return &ShardsRebuild{ShardsRebuild: response[0 : count-1], More: true, Next: next}, nil
	}
	return &ShardsRebuild{ShardsRebuild: response, More: false, Next: 0}, nil
}

//Preprocess dealing with orphan shards which has committed by SN but not tagged in rebuild service
func (rebuilder *Rebuilder) Preprocess() {
	entry := log.WithFields(log.Fields{Function: "Preprocess"})
	urls := rebuilder.Compensation.AllSyncURLs
	snCount := len(urls)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionCR := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(CPSRecordTab)
	var wg sync.WaitGroup
	wg.Add(snCount)
	for i := 0; i < snCount; i++ {
		snID := int32(i)
		go func() {
			entry.Infof("starting preprocess SN%d", snID)
			for {
				record := new(CPSRecord)
				err := collectionCR.FindOne(context.Background(), bson.M{"_id": snID}).Decode(record)
				if err != nil {
					if err == mongo.ErrNoDocuments {
						record = &CPSRecord{ID: snID, Start: 0, Timestamp: time.Now().Unix()}
						_, err := collectionCR.InsertOne(context.Background(), record)
						if err != nil {
							entry.WithError(err).Errorf("insert compensation record: %d", snID)
							time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
							continue
						}
					} else {
						entry.WithError(err).Errorf("find compensation record: %d", snID)
						time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
						continue
					}
				}
				shardsRebuilt, err := GetRebuildShards(rebuilder.httpCli, rebuilder.Compensation.AllSyncURLs[snID], record.Start, rebuilder.Compensation.BatchSize, time.Now().Unix()-int64(rebuilder.Compensation.SkipTime))
				if err != nil {
					time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
					continue
				}
				for _, sr := range shardsRebuilt.ShardsRebuild {
					collectionRS.UpdateOne(context.Background(), bson.M{"_id": sr.VFI, "timestamp": bson.M{"$lt": Int64Max}}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
				}
				if shardsRebuilt.More {
					collectionCR.UpdateOne(context.Background(), bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": shardsRebuilt.Next, "timestamp": time.Now().Unix()}})
				} else {
					if len(shardsRebuilt.ShardsRebuild) > 0 {
						next := shardsRebuilt.ShardsRebuild[len(shardsRebuilt.ShardsRebuild)-1].ID + 1
						collectionCR.UpdateOne(context.Background(), bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": next, "timestamp": time.Now().Unix()}})
					}
					break
				}
			}
			entry.Infof("finished pre-process SN%d", snID)
			wg.Done()
		}()
	}
	wg.Wait()
}

//Compensate dealing with orphan shards which has been committed by SN when service is running
func (rebuilder *Rebuilder) Compensate() {
	entry := log.WithFields(log.Fields{Function: "Compensate"})
	urls := rebuilder.Compensation.AllSyncURLs
	snCount := len(urls)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionCR := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(CPSRecordTab)
	for i := 0; i < snCount; i++ {
		snID := int32(i)
		go func() {
			entry.Infof("starting compensate SN%d", snID)
			for {
				record := new(CPSRecord)
				err := collectionCR.FindOne(context.Background(), bson.M{"_id": snID}).Decode(record)
				if err != nil {
					if err == mongo.ErrNoDocuments {
						record = &CPSRecord{ID: snID, Start: 0, Timestamp: time.Now().Unix()}
						_, err := collectionCR.InsertOne(context.Background(), record)
						if err != nil {
							entry.WithError(err).Errorf("insert compensation record: %d", snID)
							time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
							continue
						}
					} else {
						entry.WithError(err).Errorf("find compensation record: %d", snID)
						time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
						continue
					}
				}
				shardsRebuilt, err := GetRebuildShards(rebuilder.httpCli, rebuilder.Compensation.AllSyncURLs[snID], record.Start, rebuilder.Compensation.BatchSize, time.Now().Unix()-int64(rebuilder.Compensation.SkipTime))
				if err != nil {
					time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
					continue
				}
				for _, sr := range shardsRebuilt.ShardsRebuild {
					collectionRS.UpdateOne(context.Background(), bson.M{"_id": sr.VFI, "timestamp": bson.M{"$lt": Int64Max}}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
					rebuilder.Cache.Delete(sr.VFI)
				}
				if shardsRebuilt.More {
					collectionCR.UpdateOne(context.Background(), bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": shardsRebuilt.Next, "timestamp": time.Now().Unix()}})
				} else {
					if len(shardsRebuilt.ShardsRebuild) > 0 {
						next := shardsRebuilt.ShardsRebuild[len(shardsRebuilt.ShardsRebuild)-1].ID + 1
						collectionCR.UpdateOne(context.Background(), bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": next, "timestamp": time.Now().Unix()}})
					}
					time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
				}
			}
		}()
	}
}
