package main

import (
	"context"
	"flag"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/guardian/multimedia-holding-pen-utils/models"
	"log"
)

func main() {
	inputFilePtr := flag.String("input", "report.csv", "CSV report to read from")
	bucketPtr := flag.String("bucket", "holding-pen", "Bucket name that contains the original media files")
	desiredThreadsPtr := flag.Int("threads", 4, "Number of concurrent deletion operations to run")
	reallyDeletePtr := flag.Bool("really-delete", false, "Only attempt to delete files if this option is set")
	flag.Parse()

	s3config, confErr := awsconfig.LoadDefaultConfig(context.Background())
	if confErr != nil {
		log.Fatal("Could not set up default AWS config: ", confErr)
	}

	s3client := s3.NewFromConfig(s3config)

	inputCh, inputErrCh := models.AsyncCsvReader(*inputFilePtr)
	entriesCh, entryErrCh := AsyncEntryFanout(inputCh, *bucketPtr)
	deleteErrCh := AsyncEntryDeleter(s3client, entriesCh, *desiredThreadsPtr, *reallyDeletePtr)

	func() {
		for {
			select {
			case err := <-inputErrCh:
				log.Print("ERROR main received error from input reader: ", err)
				return
			case err := <-entryErrCh:
				log.Print("ERROR main received error from fanout: ", err)
				return
			case err := <-deleteErrCh:
				if err == nil {
					log.Print("INFO main deletion thread exited normally, completed")
					return
				} else {
					log.Print("ERROR main deletion thread reported an error: ", err)
				}
			}
		}
	}()

	log.Print("All done.")
}
