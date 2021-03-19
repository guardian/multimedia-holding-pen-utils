package models

import (
	"fmt"
	"net/url"
	"strings"
)

type FoundEntry struct {
	Bucket string
	Path   string
	Size   int64
}

func (e FoundEntry) ToUri() (*url.URL, error) {
	return url.Parse(fmt.Sprintf("s3://%s/%s", e.Bucket, e.Path))
}

func (e FoundEntry) MustUri() *url.URL {
	result, err := e.ToUri()
	if err != nil {
		panic(err.Error())
	}
	return result
}

type LookupResult struct {
	RequestedFile     string
	RequestedFileSize int64
	Count             int64
	Entries           []FoundEntry
	Proxies           []FoundEntry
}

func LookupResultCSVHeader() []string {
	return []string{
		"Source",
		"Duplicates count",
		"Proxy count",
		"Duplicates buckets",
		"Proxy locations",
	}
}

func (l *LookupResult) ToCSVRow() []string {
	duplicateBuckets := make([]string, len(l.Entries))
	for i, e := range l.Entries {
		duplicateBuckets[i] = e.Bucket
	}
	proxyUris := make([]string, len(l.Proxies))
	for i, p := range l.Proxies {
		proxyUris[i] = p.MustUri().String()
	}

	return []string{
		l.RequestedFile,
		fmt.Sprintf("%d", l.Count),
		fmt.Sprintf("%d", len(l.Proxies)),
		strings.Join(duplicateBuckets, "|"),
		strings.Join(proxyUris, "|"),
	}
}
