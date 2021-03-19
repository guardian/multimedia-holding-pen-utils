package main

import (
	"context"
	"flag"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olivere/elastic"
	"log"
	"math"
	"strings"
	"time"
)

func main() {
	targetBucketPtr := flag.String("target", "holding-pen", "name of the holding pen bucket to check")
	esUrlPtr := flag.String("elastic", "http://127.0.0.1:9200", "Comma-separated list of Elasticsearch addresses")
	indexNamePtr := flag.String("index", "archivehunter", "Name of the index to query")
	timeoutStringPtr := flag.String("timeout", "30s", "default network timeout")
	excludeBucketsPtr := flag.String("exclude", "", "comma-separated list of buckets to exclude")
	desiredThreadsPtr := flag.Int("threads", 4, "number of concurrent lookups to perform")
	proxyBucketPtr := flag.String("proxy", "proxies", "name of bucket to look for proxies in")
	flag.Parse()

	s3config, confErr := awsconfig.LoadDefaultConfig(context.Background())
	if confErr != nil {
		log.Fatal("Could not set up default AWS config: ", confErr)
	}

	timeout, tParseErr := time.ParseDuration(*timeoutStringPtr)
	if tParseErr != nil {
		log.Fatalf("Could not parse '%s' as a duration: %s", *timeoutStringPtr, tParseErr)
	}

	excludeBuckets := strings.Split(*excludeBucketsPtr, ",")

	if *excludeBucketsPtr == "" {
		excludeBuckets = []string{}
	}
	if len(excludeBuckets) > 0 {
		log.Printf("INFO Excluding %d other buckets from results: %v", len(excludeBuckets), excludeBuckets)
	}

	esClient, esErr := elastic.NewClient(elastic.SetURL(*esUrlPtr),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)

	if esErr != nil {
		log.Fatal("Could not connect to Elastic Search: ", esErr)
	}

	s3Client := s3.NewFromConfig(s3config)

	s3ObjectCh, errCh := AsyncReadBucket(s3Client, *targetBucketPtr, timeout)
	lookedUpCh, lookupErrCh := AsyncIndexLookup(esClient, *indexNamePtr, *targetBucketPtr, *desiredThreadsPtr, &excludeBuckets, s3ObjectCh)
	proxyLocatedCh, locatorErrCh := AsyncLocateProxy(s3Client, lookedUpCh, *proxyBucketPtr, 10)

	var totalSize int64 = 0
	var fileCount int64 = 0
	var matchedFiles int64 = 0
	var matchedSize int64 = 0
	func() {
		for {
			select {
			case file := <-proxyLocatedCh:
				if file == nil {
					log.Print("INFO main All done")
					return
				}

				//decodedFileName, decodeErr := url.QueryUnescape(*file.Key)
				//if decodeErr != nil {
				//	log.Printf("ERROR could not decode filename '%s': %s", *file.Key, decodeErr)
				//} else {
				//	log.Printf("Got %s in class %s, with size %d", decodedFileName, file.StorageClass, file.Size)
				//}
				//totalSize += file.Size
				bucketsList := make([]string, len(file.Entries))
				for i, e := range file.Entries {
					bucketsList[i] = e.Bucket
				}

				bucketsListStr := "(" + strings.Join(bucketsList, ",") + ")"
				log.Printf("INFO %s has %d results in %s and %d proxies", file.RequestedFile, file.Count, bucketsListStr, len(file.Proxies))
				totalSize += file.RequestedFileSize
				fileCount++

				if file.Count > 0 {
					matchedFiles++
					matchedSize += file.RequestedFileSize
				}
			case err := <-errCh:
				log.Print("ERROR main got error from AsyncReadBucket: ", err)
				return
			case err := <-lookupErrCh:
				log.Print("ERROR main got error from AsyncIndexLookup: ", err)
				return
			case err := <-locatorErrCh:
				log.Print("WARNING main got error from AsyncProxyLookup: ", err)
			}
		}
	}()

	totalSizeInTb := float64(totalSize) / math.Pow(1024.0, 4)
	matchedSizeInTb := float64(matchedSize) / math.Pow(1024.0, 4)
	log.Printf("All done, got a total of %0.1fTb in %d files of which %0.1fTb in %d files was matched", totalSizeInTb, fileCount, matchedSizeInTb, matchedFiles)
}
