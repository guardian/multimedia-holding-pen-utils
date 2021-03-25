package main

import (
	"github.com/guardian/multimedia-holding-pen-utils/models"
	"log"
	"net/url"
)

/**
for each record coming in, emits a models.FoundEntry for the original file and each identified proxy
*/
func AsyncEntryFanout(inputCh chan *models.LookupResult, rootBucket string) (chan *models.FoundEntry, chan error) {
	outputCh := make(chan *models.FoundEntry, 100)
	errCh := make(chan error, 1)

	go func() {
		for {
			rec := <-inputCh
			if rec == nil {
				log.Print("INFO AsyncEntryFanout got end of stream, teminating")
				outputCh <- nil
				return
			}

			rootEntry := models.FoundEntry{
				Bucket: rootBucket,
				Path:   url.QueryEscape(rec.RequestedFile),
				Size:   rec.RequestedFileSize,
			}
			outputCh <- &rootEntry
			if len(rec.Proxies) > 3 {
				log.Printf("WARNING AsyncEntryFanout %s has %d proxies which is suspiciously large, ignoring them", rec.RequestedFile, len(rec.Proxies))
			}
			for _, prox := range rec.Proxies {
				copiedEntry := prox
				outputCh <- &copiedEntry //never directly take the address of an iterator!
			}
		}
	}()
	return outputCh, errCh
}
