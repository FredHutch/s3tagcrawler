#!/bin/bash

GOOS=linux GOARCH=amd64 go build -o s3tagcrawler_linux ./main.go
