# csv-concurrency-go
Setup
1. Create folder structure  
in_csv/app-records.csv  
logging/  
out_csv/  

Execution (from command line)
1. Do go run on the go file, and pass in the number of go routines you want the records spread evenly accross.
go run process-csv-conurrect.go 5

Details:
If you pass in a number of go routines that exceeds the number of records, the number you've requested will be reset to the number of records.
Eg: 5 records, 6 requested go routines will turn into 5 requested go routines.


