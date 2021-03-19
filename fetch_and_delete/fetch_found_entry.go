package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/guardian/multimedia-holding-pen-utils/models"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

/**
checks for the existence of a local file. Returns the file size (if found), true if found/false if not or
an error if the stat operation failed
*/
func localFileSize(filepath string) (int64, bool, error) {
	statInfo, statErr := os.Stat(filepath)
	if statErr == nil {
		return statInfo.Size(), true, nil
	} else {
		if os.IsNotExist(statErr) {
			return 0, false, nil
		} else {
			return 0, false, statErr
		}
	}
}

/**
ensures that the local directories are created
*/
func createLocalDir(forPath string) error {
	dirs := path.Dir(forPath)
	return os.MkdirAll(dirs, 0750)
}

/**
performs a download of the given s3 object to the local filepath. Returns the number of bytes downloaded or an error.
if a file already exists that is the _same_ size as the remote target, then the return value is the same as if the
file had been downloaded.  if a file already exists that is _not_ the same size then an error is returned.
*/
func performDownload(s3Client *s3.Client, bucket string, key string, toFile string) (int64, error) {
	req := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	response, err := s3Client.GetObject(ctx, &req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	localLength, doesExist, statErr := localFileSize(toFile)
	if statErr != nil {
		return 0, statErr
	}

	if doesExist && localLength == response.ContentLength {
		log.Printf("INFO performDownload local file %s already exists with the right file size", toFile)
		return localLength, nil
	} else if doesExist {
		log.Printf("ERROR performDownload local file %s exists with size %d but remote has size %d", toFile, localLength, response.ContentLength)
		return 0, errors.New("another file exists already")
	}

	dirErr := createLocalDir(toFile)
	if dirErr != nil {
		log.Printf("ERROR performDownload could not create directories for '%s': %s", toFile, dirErr)
		return 0, dirErr
	}

	file, openErr := os.OpenFile(toFile, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0640)
	if openErr != nil {
		return 0, openErr
	}
	defer file.Close()

	bytesCopied, copyErr := io.Copy(file, response.Body)
	if copyErr != nil {
		defer os.Remove(toFile)
		return 0, copyErr
	}
	if bytesCopied != response.ContentLength {
		defer os.Remove(toFile)
		return 0, errors.New(fmt.Sprintf("Incorrect number of bytes read/written, expected %d got %d", response.ContentLength, bytesCopied))
	}

	return bytesCopied, nil
}

func fetcherThread(s3Client *s3.Client, inputCh chan *models.FoundEntry, outputCh chan *models.FoundEntry, errCh chan error, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for {
		rec := <-inputCh
		if rec == nil {
			log.Print("INFO fetcherThread received end of stream, exiting")
			return
		}

		keyToUse := rec.Path
		if strings.HasPrefix(keyToUse, "/") {
			keyToUse = keyToUse[1:]
		}

		if strings.Contains(keyToUse, "%2F") {
			decoded, decErr := url.QueryUnescape(keyToUse)
			if decErr != nil {
				log.Printf("ERROR fetcherThread could not urldecode '%s': %s", keyToUse, decErr)
				continue
			}
			keyToUse = decoded
		}

		localPath := path.Join("media", keyToUse)
		if rec.IsProxy {
			localPath = path.Join("proxy", keyToUse)
		}

		bytesCopied, err := performDownload(s3Client, rec.Bucket, keyToUse, localPath)
		if err != nil {
			log.Printf("ERROR fetcherThread can't download %s:%s - %s", rec.Bucket, keyToUse, err)
			errCh <- err
			return
		} else {
			downloadedMb := float64(bytesCopied) / math.Pow(1024, 2)
			log.Printf("INFO fetcherThread %s %.1fMb", keyToUse, downloadedMb)
			outputCh <- rec
		}
	}
}

func AsyncItemFetcher(s3Client *s3.Client, inputCh chan *models.FoundEntry, threads int) (chan *models.FoundEntry, chan error) {
	outputCh := make(chan *models.FoundEntry, 100)
	modifiedInputCh := make(chan *models.FoundEntry, 100)
	errCh := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	go func() {
		for {
			rec := <-inputCh
			if rec == nil {
				log.Print("INFO AsyncItemFetcher reached end of stream, sending EOS to workers")
				for i := 0; i < threads; i++ {
					modifiedInputCh <- nil
				}
				waitGroup.Wait()
				log.Print("INFO AsyncItemFetcher all workers shut down, exiting")
				return
			} else {
				if rec.Bucket != "" && rec.Path != "" {
					modifiedInputCh <- rec
				}
			}
		}
	}()

	for i := 0; i < threads; i++ {
		go fetcherThread(s3Client, modifiedInputCh, outputCh, errCh, waitGroup)
		waitGroup.Add(1)
	}

	return outputCh, errCh
}
