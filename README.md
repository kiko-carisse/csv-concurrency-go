# csv-concurrency-go
Setup
1. Create folder structure  
in_csv/all-records.csv  
logging/  
out_csv/  
2. The code is runnable at this point. It will hit json placeholder api and return success results in the out_csv/.  


Execution (from command line)
1. Do go run on the go file, and pass in the number of go routines you want the records spread evenly accross.  
go run process-csv-conurrect.go 5

Details:
- If you pass in a number of go routines that exceeds the number of records, the number you've requested will be reset to the number of records.  
Eg: 5 records with 6 requested go routines will turn into 5 requested go routines.  
- writeToLoggingResults() function is used to output general logging in the logging folder. There's 1 file generated for each go routine.  
- API return errors are logged in logging/-api-errors.csv files. There will be 1 error file generated per go routine while the program is running (if errors occur), and those errors will be aggregated automatically into one cosolidated error api error file at the end called logging/api-errors.csv. This csv file will contain information about the api request and response, such as the full request body and response body, the http status code, possibly a go exception if one occured during the api call, etc.  


