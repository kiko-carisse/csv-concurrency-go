package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/segmentio/ksuid"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// recordRespDetailsDS ...
type recordRespDetailsDS struct {
	ID int `json:"id"`
}

func main() {

	// This is the number of go routines requested to spread all records evenly across
	numOfRequestedRTs, _ := strconv.Atoi(os.Args[1])
	allConcurrentRecordsStartTime := time.Now().Unix()

	currentDateTimeStamp := timeTZ(time.Now(), "America/Vancouver").Format("2006-01-02 3:04:05 PM")
	writeToLoggingResults("Start All Concurrent Records: "+currentDateTimeStamp+" \n\n", 0, false, "total-execution-time")

	totalRowCount := countCSVRows()

	if totalRowCount < numOfRequestedRTs {
		numOfRequestedRTs = totalRowCount
	}

	// recordsPerRT is the number of records that will be provided to each routine
	var recordsPerRT int = totalRowCount / numOfRequestedRTs

	csvfile, err := os.Open("in_csv/all-records.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// READING CSV RECORDS. RECORD LOOP.
	r := csv.NewReader(csvfile)
	firstCsvLine := true

	headerRow := []string{}
	dataRowsHeap := [][]string{}
	// h is a helper map to use with the headerRow to more easily access column data by col header name
	h := make(map[string]int)

	rtNum := 1 // Give each go routine an identity using an int. Largely used for parallel file writing.
	rowNum := 1

	var wg sync.WaitGroup
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if firstCsvLine { // for the first line, gather column header names of the csv, so we can grab the data by column name.

			// MAKE SURE ALL DEPENDANT COLUMNS EXIST BEFORE PROCEEDING -
			// if a non existent column is referenced, it will return zero value for the map of ints every time. Which means it will return the first column every time.
			// So we are trying to avoid that here by making sure the column header names exist before proceeding.

			for x := 0; x < len(record); x++ {
				h[record[x]] = x
			}

			requiredColumnHeaders := []string{"col_one", "col_two", "col_three"}

			for _, columnName := range requiredColumnHeaders {
				if _, ok := h[columnName]; !ok {
					log.Fatalln("\"" + columnName + "\" column header missing from csv.")
				}
			}

			headerRow = append(headerRow, record...)

		} else if !firstCsvLine { // Now proceed with the record set.

			// Append the records onto a heap of records, then when they reach a certain number, dump them off onto a go routine
			dataRowsHeap = append(dataRowsHeap, record)

			if rowNum%recordsPerRT == 0 && rtNum < numOfRequestedRTs {

				// Dump the records onto go routine.
				wg.Add(1)
				go func(rtNum int, headerRow []string, dataRowsHeap [][]string) {
					defer wg.Done()

					// process the heap records
					processRecordsHeap(rtNum, headerRow, dataRowsHeap, h)

				}(rtNum, headerRow, dataRowsHeap)

				// after dumping records, create a new array to start building up a heap of records for the next go routine to process.
				dataRowsHeap = [][]string{}
				rtNum++
			}
			rowNum++

		}

		firstCsvLine = false

	}

	// Take the last heap of records, which will be the quotient + remainder of records.
	if len(dataRowsHeap) > 0 {

		wg.Add(1)
		go func(rtNum int, headerRow []string, dataRowsHeap [][]string) {
			defer wg.Done()

			// process the records
			processRecordsHeap(rtNum, headerRow, dataRowsHeap, h)

		}(rtNum, headerRow, dataRowsHeap)

	}

	// Wait for all processRecordsHeap(), go routines, to complete
	wg.Wait()

	csvfile.Close()

	aggregateOutCSVs(rtNum)      // aggregate out_csv files.
	aggregateAPIErrorCSVs(rtNum) // aggregate logging error csv files.

	// log how long all the go routines took altogether
	allConcurrentRecordsCompleteTime := int(time.Now().Unix() - allConcurrentRecordsStartTime)
	writeToLoggingResults("Finished All Concurrent Records \nTotal Execution Time: "+strconv.Itoa(allConcurrentRecordsCompleteTime)+" seconds \n\n\n\n", 0, true, "total-execution-time")

}

func processRecordsHeap(rtNum int, headerRow []string, dataRowsHeap [][]string, h map[string]int) {

	allRecordsStartTime := time.Now().Unix()
	writeToLoggingResults("Start All Records from Routine #"+strconv.Itoa(rtNum)+": \n\n", rtNum, false, "results")

	numOfRecordsProcessed := 0

	headerRowOut := append([]string{}, headerRow...)
	headerRowOut = append(headerRowOut, []string{"col_four_api_returned"}...)
	writeToOutCSV(headerRowOut, rtNum, false)

	// READING RECORDS FROM DATA HEAP. RECORD LOOP
	for _, dataRow := range dataRowsHeap {
		func() {
			defer func() { // If there's an issue with the data, or API call, this will allow it to continue onto the next record. API errors will be logged separately in error csv
				if err := recover(); err != nil {
					fmt.Println(err)
				}
			}()

			// set dynamic vars
			colOneSTRID := dataRow[h["col_one"]]
			colTwoINT, _ := strconv.Atoi(dataRow[h["col_two"]])
			colThreeFL, _ := strconv.ParseFloat(dataRow[h["col_three"]], 64)

			processedVar := float64(colTwoINT) * colThreeFL

			//API call
			var recordRespDetails recordRespDetailsDS
			sendRecordAPICall(colOneSTRID, processedVar, &recordRespDetails, rtNum)

			dataOutputRow := append(dataRow, []string{strconv.Itoa(recordRespDetails.ID)}...)
			writeToOutCSV(dataOutputRow, rtNum, true)

			numOfRecordsProcessed++
		}()

	}

	allRecordsCompleteTime := int(time.Now().Unix() - allRecordsStartTime)
	writeToLoggingResults("Finished All Records for routine #"+strconv.Itoa(rtNum)+" \nNum of Records Processed: "+strconv.Itoa(numOfRecordsProcessed)+" \nTotal Execution Time: "+strconv.Itoa(allRecordsCompleteTime)+" seconds \n\n\n\n", rtNum, true, "results")

}

func sendRecordAPICall(colOneSTRID string, processedVar float64, recordRespDetails *recordRespDetailsDS, rtNum int) {

	recordOnboardAPIStartTime := time.Now().Unix()

	url := "https://jsonplaceholder.typicode.com/posts"

	id, err := strconv.Atoi(colOneSTRID)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	dataST := struct {
		ColID        int     `json:"colID"`
		ProcessedVar float64 `json:"processedVar"`
	}{
		ColID:        id,
		ProcessedVar: processedVar,
	}

	jsonStr, _ := json.Marshal(dataST)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Authorization", "Bearer <token_var_here>")
	req.Header.Set("Content-Type", "application/json")

	// Add a unique id for request tracing
	reqUUID := genKsuid()
	req.Header.Set("X-Request-ID", reqUUID)

	client := &http.Client{}
	resp, err := client.Do(req)

	currentDateTimeStamp := timeTZ(time.Now(), "America/Vancouver").Format("2006-01-02 3:04:05 PM")

	if err != nil {
		dataRow := []string{strconv.Itoa(rtNum), reqUUID, colOneSTRID, "", "sendRecordAPICall()", url, "", string(jsonStr), "", err.Error(), currentDateTimeStamp}
		writeToAPIErrorsCSV(dataRow, rtNum, true)
		panic("API Exception Error: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		dataRow := []string{strconv.Itoa(rtNum), reqUUID, colOneSTRID, "", "sendRecordAPICall()", url, resp.Status, string(jsonStr), string(responseBody), "", currentDateTimeStamp}
		writeToAPIErrorsCSV(dataRow, rtNum, true)
		panic(resp.Status + ": " + reqUUID)
	}

	// Print http response
	fmt.Println(resp.Status)

	respBody, _ := ioutil.ReadAll(resp.Body)

	json.Unmarshal(respBody, &recordRespDetails)

	recordOnboardAPIEndTime := int(time.Now().Unix() - recordOnboardAPIStartTime)
	writeToLoggingResults("\n "+strconv.Itoa(+recordRespDetails.ID)+" "+colOneSTRID+" sendRecordAPICall() api call completed in "+strconv.Itoa(recordOnboardAPIEndTime)+" seconds \n", rtNum, true, "results")
}

// ===================================================
// API Error CSV logging functions

// writeToLoggingResults writes to general output logs
func writeToLoggingResults(output string, rtNum int, append bool, fileSuffix string) {
	// If the file doesn't exist, create it, or append to the file
	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	f, err := os.OpenFile("logging/"+strconv.Itoa(rtNum)+"-"+fileSuffix+".txt", writeOptions, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte(output)); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

// writeToAPIErrorsCSV writes single error rows
func writeToAPIErrorsCSV(dataRow []string, rtNum int, append bool) {

	csvDataRows := [][]string{dataRow}
	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile("logging/"+strconv.Itoa(rtNum)+"-errors.csv", writeOptions, 0644)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	w := csv.NewWriter(file)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}

}

// aggregateAPIErrorCSVs collects all records from all api errors csv's, and passes them to writeAggregateToCombinedAPIErrorResultsCSV
func aggregateAPIErrorCSVs(numCSVs int) {

	//Empty csv if it exists already
	headerRow := []string{"rtNum", "X-Request-ID", "TLapp_AppID", "recordNumber", "method", "Endpoint", "Http_Response_Code", "Request_Body", "Response_Body", "Exception_Message", "DateTime_Stamp"}

	writeAggregateToCombinedAPIErrorResultsCSV([][]string{headerRow}, false)

	for csvNum := 1; csvNum <= numCSVs; csvNum++ {

		func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
			defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
				if ex := recover(); ex != nil {
					// Silent fail
					// fmt.Println(fmt.Errorf("%v", ex))
				}
			}()

			csvfile, err := os.Open("logging/" + strconv.Itoa(csvNum) + "-errors.csv")
			if err != nil {
				panic(err)
			}

			r := csv.NewReader(csvfile)

			dataRowsHeap := [][]string{}

			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
					defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
						if ex := recover(); ex != nil {
							fmt.Errorf("%v", ex)
						}
					}()

					dataRowsHeap = append(dataRowsHeap, record)

				}()

			}

			writeAggregateToCombinedAPIErrorResultsCSV(dataRowsHeap, true)

			csvfile.Close()

			// after scraping it, delete it.
			var errDel = os.Remove("logging/" + strconv.Itoa(csvNum) + "-errors.csv")
			if errDel != nil {
				fmt.Println(errDel.Error())
			}
		}()

	}

}

// writeAggregateToCombinedAPIErrorResultsCSV writes all api error records into one api error file.
func writeAggregateToCombinedAPIErrorResultsCSV(csvDataRows [][]string, append bool) {

	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile("logging/output-errors.csv", writeOptions, 0644)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	w := csv.NewWriter(file)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

// ===================================================
// Out Success CSV logging functions

// writeToOutCSV writes a copy of the records from the in_csv/all-records.csv, with any optional new columns appended returned from api
func writeToOutCSV(dataRow []string, rtNum int, append bool) {

	csvDataRows := [][]string{dataRow}
	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile("out_csv/"+strconv.Itoa(rtNum)+".csv", writeOptions, 0644)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	w := csv.NewWriter(file)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}

}

// aggregateOutCSVs combines the csv records form writeToOutCSV and passes them to writeAggregateToCombinedOutCSV
func aggregateOutCSVs(numCSVs int) {

	//Empty csv if it exists already
	writeAggregateToCombinedOutCSV([][]string{}, false)

	firstCSV := true

	for csvNum := 1; csvNum <= numCSVs; csvNum++ {

		func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
			defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
				if ex := recover(); ex != nil {
					fmt.Println(ex)
				}
			}()

			csvfile, err := os.Open("out_csv/" + strconv.Itoa(csvNum) + ".csv")
			if err != nil {
				log.Fatalln("Couldn't open the csv file", err)
			}

			r := csv.NewReader(csvfile)

			dataRowsHeap := [][]string{}

			firstCsvLine := true
			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
					defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
						if ex := recover(); ex != nil {
							fmt.Errorf("%v", ex)
						}
					}()

					if firstCsvLine && firstCSV { // first line is just header names
						dataRowsHeap = append(dataRowsHeap, record)
					} else if !firstCsvLine {
						dataRowsHeap = append(dataRowsHeap, record)
					}
				}()
				firstCsvLine = false
			}

			writeAggregateToCombinedOutCSV(dataRowsHeap, true)

			csvfile.Close()

			// after scraping it, delete it.
			var errDel = os.Remove("out_csv/" + strconv.Itoa(csvNum) + ".csv")
			if errDel != nil {
				fmt.Println(errDel.Error())
			}
		}()
		firstCSV = false
	}

}

// writeAggregateToCombinedOutCSV writes all out records into one file.
func writeAggregateToCombinedOutCSV(csvDataRows [][]string, append bool) {

	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile("out_csv/onboarded-records.csv", writeOptions, 0644)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	w := csv.NewWriter(file)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

// ===================================================
// Utility functions

// countCSVRows counts the rows of in_csv/all-records.csv
func countCSVRows() int {

	csvfile, err := os.Open("in_csv/all-records.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	r := csv.NewReader(csvfile)
	firstCsvLine := true
	// READING CSV RECORDS. LOAN LOOP

	rowCount := 0

	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if !firstCsvLine {
			rowCount++
		}

		firstCsvLine = false
	}

	csvfile.Close()

	return rowCount

}

// floatToString rounds a float to 2 decimals and converts to string for ease of saving to csv.
func floatToString(inputNum float64) string {
	// to convert a float number to a string
	return strconv.FormatFloat(inputNum, 'f', 2, 64)
}

// timeTZ casts time object into specified timezone.
func timeTZ(t time.Time, name string) time.Time {
	loc, err := time.LoadLocation(name)
	if err == nil {
		t = t.In(loc)
	} else {
		panic("Issue parsing time into requested timezone" + err.Error())
	}
	return t
}

// genKsuid simply a shortened way of getting uuid from ksuid package.
func genKsuid() string {
	return ksuid.New().String()
}
