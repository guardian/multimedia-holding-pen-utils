package main

import (
	"encoding/csv"
	"github.com/guardian/multimedia-holding-pen-utils/models"
	"log"
	"os"
)

func AsyncOutputWriter(filename string, onlyWithDupes bool, inputCh chan *models.LookupResult) chan error {
	errCh := make(chan error, 1)

	go func() {
		file, openErr := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
		if openErr != nil {
			log.Printf("ERROR can't open %s to write: %s", filename, openErr)
			errCh <- openErr
			return
		}
		defer file.Close()

		csvWriter := csv.NewWriter(file)
		defer csvWriter.Flush()

		headerErr := csvWriter.Write(models.LookupResultCSVHeader())
		if headerErr != nil {
			errCh <- headerErr
			return
		}

		for {
			rec := <-inputCh
			if rec == nil {
				log.Print("AsyncOutputWriter reached end of stream, terminating")
				errCh <- nil
				return
			}
			if onlyWithDupes && rec.Count == 0 {
				continue
			}

			err := csvWriter.Write(rec.ToCSVRow())
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
	return errCh
}
