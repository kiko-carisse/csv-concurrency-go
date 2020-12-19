SHELL := /bin/bash

tidy:
	go mod tidy
	go mod vendor
