package models

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
)

type FoundEntry struct {
	Bucket  string
	Path    string
	Size    int64
	IsProxy bool
}

func FoundEntryFromUri(from *url.URL, isProxy bool) (*FoundEntry, error) {
	if from == nil {
		return nil, errors.New("no data provided")
	}
	decodedPath, decodeErr := url.QueryUnescape(from.Path)
	if decodeErr != nil {
		return nil, decodeErr
	}
	return &FoundEntry{
		Bucket:  from.Host,
		Path:    decodedPath,
		Size:    0,
		IsProxy: isProxy,
	}, nil
}

func FoundEntryFromUriString(from string, isProxy bool) (*FoundEntry, error) {
	parsedUrl, parseErr := url.Parse(from)
	if parseErr != nil {
		return nil, parseErr
	}
	return FoundEntryFromUri(parsedUrl, isProxy)
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

func LookupResultFromCSVRow(row *[]string) (*LookupResult, error) {
	if row == nil {
		return nil, errors.New("no data was provided")
	}

	if len(*row) < 5 {
		return nil, errors.New("not enough columns, need 5")
	}

	dupCount, dupCountErr := strconv.ParseInt((*row)[1], 10, 64)
	if dupCountErr != nil {
		return nil, dupCountErr
	}

	//proxCount, proxCountErr := strconv.ParseInt((*row)[2], 10, 64)
	//if proxCountErr != nil {
	//	return nil, proxCountErr
	//}

	proxyUrlStrings := strings.Split((*row)[4], "|")
	proxies := make([]FoundEntry, len(proxyUrlStrings))
	for i, urlString := range proxyUrlStrings {
		foundEntryPtr, err := FoundEntryFromUriString(urlString, true)
		if err != nil {
			log.Printf("ERROR could not interpret proxy %d on %s: %s", i, (*row)[0], err)
			return nil, err
		}
		proxies[i] = *foundEntryPtr
	}

	entryBuckets := strings.Split((*row)[3], "|")
	entries := make([]FoundEntry, len(entryBuckets))
	for i, buck := range entryBuckets {
		entries[i].Path = (*row)[0]
		entries[i].Bucket = buck
		entries[i].IsProxy = false
	}

	rec := &LookupResult{
		RequestedFile:     (*row)[0],
		RequestedFileSize: 0,
		Count:             dupCount,
		Entries:           entries,
		Proxies:           proxies,
	}
	return rec, nil
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
