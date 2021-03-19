package main

import (
	"context"
	"encoding/json"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/guardian/multimedia-holding-pen-utils/models"
	"github.com/olivere/elastic"
	"log"
	"net/url"
	"sync"
)

type FoundEntry struct {
	Bucket string
	Path   string
	Size   int64
}

type LookupResult struct {
	RequestedFile     string
	RequestedFileSize int64
	Count             int64
	Entries           []FoundEntry
	Proxies           []FoundEntry
}

/**
builds a query looking for the file path in all other buckets
*/
func makeQuery(filepath string, targetBucket string, excludeBucketsPtr *[]string) *elastic.BoolQuery {
	excludes := []elastic.Query{
		elastic.NewTermQuery("bucket.keyword", targetBucket),
	}

	if excludeBucketsPtr != nil {
		for _, toExclude := range *excludeBucketsPtr {
			excludes = append(excludes, elastic.NewTermQuery("bucket.keyword", toExclude))
		}
	}
	return elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("path.keyword", filepath),
	).MustNot(
		excludes...,
	)
}

func lookupProcessor(esClient *elastic.Client,
	indexName string,
	targetBucket string,
	excludeBucketsPtr *[]string,
	inputCh chan *awstypes.Object,
	outputCh chan *LookupResult,
	errCh chan error,
	waitGroup *sync.WaitGroup) {

	defer waitGroup.Done()

	ctx := context.Background()
	//ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	//defer cancelFunc()

	for {
		rec := <-inputCh
		if rec == nil {
			log.Print("DEBUG lookupProcessor terminating at end of stream")
			return
		}

		decodedFilename, decodeErr := url.QueryUnescape(*rec.Key)
		if decodeErr != nil {
			log.Printf("ERROR lookupProcessor can't urldecode '%s': %s", *rec.Key, decodeErr)
			continue
		}

		q := makeQuery(decodedFilename, targetBucket, excludeBucketsPtr)
		response, searchErr := esClient.Search(indexName).Query(q).Do(ctx)
		if searchErr != nil {
			log.Printf("ERROR lookupProcessor can't search: %s", searchErr)
			errCh <- searchErr
			return
		}

		entryList := make([]FoundEntry, len(response.Hits.Hits))

		for i, hit := range response.Hits.Hits {
			var archiveEntry models.ArchiveEntry
			unmarshalErr := json.Unmarshal(*hit.Source, &archiveEntry)
			if unmarshalErr != nil {
				log.Print("ERROR could not unmarshal index result to archive entry: ", unmarshalErr)
			} else {
				entryList[i].Bucket = archiveEntry.Bucket
				entryList[i].Path = archiveEntry.Path
				entryList[i].Size = archiveEntry.Size
			}
		}

		result := &LookupResult{
			RequestedFile:     decodedFilename,
			RequestedFileSize: rec.Size,
			Count:             response.TotalHits(),
			Entries:           entryList,
		}
		outputCh <- result
	}
}

func AsyncIndexLookup(esClient *elastic.Client,
	indexName string,
	targetBucket string,
	threads int,
	excludeBucketsPtr *[]string,
	inputCh chan *awstypes.Object) (chan *LookupResult, chan error) {

	outputCh := make(chan *LookupResult, 10)
	errCh := make(chan error, 1)
	internalErrCh := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	modifiedInputCh := make(chan *awstypes.Object, 100)

	//the input thread sends a single NULL when it has completed, but we must duplicate this for each of our goroutines
	//don't duplicate anything _else_ though
	go func() {
		for {
			select {
			case rec := <-inputCh:
				if rec == nil {
					for i := 0; i < threads; i++ {
						modifiedInputCh <- nil
					}
					log.Print("DEBUG AsyncIndexLookup sent termination signal, waiting for threads to terminate...")
					waitGroup.Wait()
					log.Print("DEBUG AsyncIndexLookup threads have terminated, now exiting")
					outputCh <- nil
				} else {
					modifiedInputCh <- rec
				}
			case err := <-internalErrCh:
				log.Print("WARNING AsyncIndexLookup received an error, terminating all threads")
				for i := 0; i < threads; i++ {
					modifiedInputCh <- nil
				}
				errCh <- err
				return
			}
		}
	}()

	for i := 0; i < threads; i++ {
		go lookupProcessor(esClient, indexName, targetBucket, excludeBucketsPtr, modifiedInputCh, outputCh, internalErrCh, waitGroup)
		waitGroup.Add(1)
	}

	return outputCh, errCh
}
