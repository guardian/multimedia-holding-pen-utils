package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/guardian/multimedia-holding-pen-utils/models"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"
)

func requestDelete(s3Client *s3.Client, bucket string, key string, timeout time.Duration) (*s3.DeleteObjectOutput, error) {
	req := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	return s3Client.DeleteObject(ctx, req)
}

func deleterThread(s3Client *s3.Client, inputCh chan *models.FoundEntry,
	errCh chan error, reallyDelete bool, waitGroup *sync.WaitGroup) {

	defer waitGroup.Done()

	for {
		entry := <-inputCh
		if entry == nil {
			log.Print("INFO deleterThread got end of stream, exiting")
			return
		}

		if entry.Bucket == "" || entry.Path == "" {
			continue
		}
		keyToUse := entry.Path
		if strings.HasPrefix(keyToUse, "/") {
			keyToUse = keyToUse[1:]
		}

		if strings.Contains(keyToUse, "%2F") {
			decodedPath, decodeErr := url.QueryUnescape(entry.Path)
			if decodeErr != nil {
				log.Printf("WARNING deleterThread could not unescape incoming path '%s': %s", entry.Path, decodeErr)
				continue
			}
			keyToUse = decodedPath
		}
		log.Printf("INFO deleterThread request to delete %s on %s", keyToUse, entry.Bucket)
		if reallyDelete {
			_, deleteErr := requestDelete(s3Client, entry.Bucket, keyToUse, 3*time.Second)
			if deleteErr != nil {
				log.Printf("ERROR deleteThread could not delete %s:%s - %s", entry.Bucket, keyToUse, deleteErr)
				errCh <- deleteErr
				return
			}
		} else {
			log.Print("INFO deleterThread not performing deletions unless --really-delete option is set")
		}
	}
}

func AsyncEntryDeleter(s3Client *s3.Client, inputCh chan *models.FoundEntry, threads int, reallyDelete bool) chan error {
	modifiedInputCh := make(chan *models.FoundEntry, 100)
	errCh := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	//interceptor stage to fanout end-of-stream marker to all workers
	go func() {
		for {
			rec := <-inputCh

			if rec == nil {
				log.Print("INFO AsyncEntryDeleter got end-of-stream, signalling subthreads")
				for i := 0; i < threads; i++ {
					modifiedInputCh <- nil
				}
				waitGroup.Wait()
				log.Print("INFO AsyncEntryDeleter all subthreads exited, now terminating")
				errCh <- nil
				return
			} else {
				modifiedInputCh <- rec
			}
		}
	}()

	for i := 0; i < threads; i++ {
		go deleterThread(s3Client, modifiedInputCh, errCh, reallyDelete, waitGroup)
		waitGroup.Add(1)
	}
	return errCh
}
