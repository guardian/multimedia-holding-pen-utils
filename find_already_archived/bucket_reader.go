package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"time"
)

func listObjectsWithTimeout(client *s3.Client, req *s3.ListObjectsV2Input, requestTimeout time.Duration) (*s3.ListObjectsV2Output, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), requestTimeout)
	defer cancelFunc()

	return client.ListObjectsV2(ctx, req)
}

/**
reads from the given bucket in the background and passes the results onto a channel
*/
func AsyncReadBucket(client *s3.Client, bucketName string, requestTimeout time.Duration) (chan *types.Object, chan error) {
	outputCh := make(chan *types.Object, 100)
	errCh := make(chan error, 1)

	go func() {
		var maybeToken *string = nil

		for {
			req := s3.ListObjectsV2Input{
				Bucket:            aws.String(bucketName),
				ContinuationToken: maybeToken,
				EncodingType:      types.EncodingTypeUrl,
			}
			response, s3err := listObjectsWithTimeout(client, &req, requestTimeout)
			if s3err != nil {
				log.Printf("ERROR AsyncReadBucket can't iterate %s: %s", bucketName, s3err)
				errCh <- s3err
				return
			}

			log.Printf("DEBUG AsyncReadBucket keycount is %d, isTruncated %v, continuationToken %v", response.KeyCount, response.IsTruncated, response.NextContinuationToken)

			for _, entry := range response.Contents {
				copiedObject := entry
				outputCh <- &copiedObject
			}

			if response.NextContinuationToken == nil {
				log.Printf("INFO AsyncReadBucket completed iterating %s", bucketName)
				outputCh <- nil
				return
			} else {
				log.Print("DEBUG AsyncReadBucket getting next page")
				maybeToken = response.NextContinuationToken
			}
		}

	}()
	return outputCh, errCh
}
