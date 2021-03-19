package models

import (
	"encoding/csv"
	"io"
	"log"
	"os"
)

func AsyncCsvReader(filename string) (chan *LookupResult, chan error) {
	outputCh := make(chan *LookupResult, 100)
	errCh := make(chan error, 1)

	go func() {
		file, openErr := os.Open(filename)
		if openErr != nil {
			errCh <- openErr
			return
		}
		defer file.Close()
		reader := csv.NewReader(file)

		lineCounter := 0
		for {
			row, readErr := reader.Read()
			if readErr != nil {
				if readErr == io.EOF {
					log.Print("INFO AsyncCsvReader reached end of file, exiting")
					outputCh <- nil
					return
				} else {
					errCh <- readErr
					return
				}
			}

			if lineCounter == 0 { //skip the header row
				lineCounter++
				continue
			}
			lineCounter++

			result, marshalErr := LookupResultFromCSVRow(&row)
			if marshalErr != nil {
				log.Printf("ERROR AsyncCsvReader could not read line %d: %s", lineCounter, marshalErr)
				continue
			} else {
				outputCh <- result
			}
		}
	}()

	return outputCh, errCh
}
