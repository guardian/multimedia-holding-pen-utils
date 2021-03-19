package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"regexp"
	"sync"
	"time"
)

/**
strip any file extension to get the prefix to search on for proxies
*/
func prefixFromFilename(filename string) (string, bool) {
	xtractor := regexp.MustCompile("^(.*)\\.([^.]+)$")
	matches := xtractor.FindAllStringSubmatch(filename, -1)
	if matches == nil {
		return filename, false
	} else {
		return matches[0][1], true
	}
}

/**
make a list request to the given bucket name with a timeout
*/
func makeSearchRequest(s3Client *s3.Client, proxybucket string, rec *LookupResult, timeout time.Duration) (*s3.ListObjectsV2Output, error) {
	prefix, gotPrefix := prefixFromFilename(rec.RequestedFile)
	if !gotPrefix {
		log.Printf("WARNING ProxyLocator.makeSearchRequest - could not get prefix from '%s'", rec.RequestedFile)
	}

	req := s3.ListObjectsV2Input{
		Bucket:  aws.String(proxybucket),
		MaxKeys: 100,
		Prefix:  &prefix,
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	return s3Client.ListObjectsV2(ctx, &req)
}

/**
makes a list of []FoundEntry from the given S3 response
*/
func proxyListFromResponse(response *s3.ListObjectsV2Output, bucketName string) []FoundEntry {
	entries := make([]FoundEntry, len(response.Contents))

	for i, obj := range response.Contents {
		entries[i].Path = *obj.Key
		entries[i].Size = obj.Size
		entries[i].Bucket = bucketName
	}
	return entries
}

func proxyLocator(s3Client *s3.Client, proxybucket string, inputCh chan *LookupResult, outputCh chan *LookupResult, errCh chan error) {
	for {
		rec := <-inputCh
		if rec == nil {
			log.Print("INFO proxyLocator thread got nil, terminating")
			return
		}
		response, searchErr := makeSearchRequest(s3Client, proxybucket, rec, 30*time.Second)
		if searchErr != nil {
			errCh <- searchErr
			continue
		}

		if response.KeyCount != 0 {
			log.Printf("DEBUG proxyLocator found %d proxies for %s", response.KeyCount, rec.RequestedFile)
			rec.Proxies = proxyListFromResponse(response, proxybucket)
		}
		outputCh <- rec
	}
}

func AsyncLocateProxy(s3Client *s3.Client, inputCh chan *LookupResult, proxybucket string, threads int) (chan *LookupResult, chan error) {
	outputCh := make(chan *LookupResult, 100)
	errCh := make(chan error, 1)
	modifiedInputCh := make(chan *LookupResult, 100)
	waitGroup := &sync.WaitGroup{}

	/**
	interceptor to make sure that when we receive a nil input (termination request) we duplicate it over
	all available threads
	*/
	go func() {
		for {
			rec := <-inputCh
			if rec == nil {
				for i := 0; i < threads; i++ {
					modifiedInputCh <- nil
				}
				log.Print("INFO AsyncLocateProxy terminating, waiting for worker threads")
				waitGroup.Wait()
				log.Print("INFO AsyncTerminateProxy all worker threads completed, exiting")
				outputCh <- nil
				return
			} else {
				modifiedInputCh <- rec
			}
		}
	}()

	for i := 0; i < threads; i++ {
		go proxyLocator(s3Client, proxybucket, modifiedInputCh, outputCh, errCh)
	}
	return outputCh, errCh
}
