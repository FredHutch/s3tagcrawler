package main

/*
NOTE: If you get a "Too many open files" error when running this, change your ulimit:
oldulimit=$(ulimit -n)
ulimit -n 4096
ulimit -n $oldulimit
TODO: tune the program so this is not needed.
*/

// FIXME if a file already exists in S3, don't overwrite it, print a message
// with the object name and go on to the next.

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	tagOnly = kingpin.Flag("tag-only", "Just tag, don't upload").Short('t').Bool()
	csvFile = kingpin.Arg("csv", "CSV file").Required().String()
)

// DataType determines whether this is array or sequencing data
type DataType string

// Data types
const (
	ArrayData      DataType = "0"
	SequencingData DataType = "1"
)

// CSVRecord represents a row in the csv manifest
type CSVRecord struct { // second iteration: 17-12-24-TagManifestUpdated.csv
	molecularID      string
	assayMaterialID  string
	s3TransferBucket string
	s3Prefix         string
	localDir         string
	stage            string
	omicsSampleName  string
	dataType         DataType
}

// file: seq_dir,s3transferbucket,s3_prefix,molecular_id,assay_material_id,stage
// struct: molecular_id,assay_material_id,s3transferbucket,s3_prefix,seq_dir,,,,,stage

// Uploads 100 files with random-ish tags
func uploadFilesWithTags() {
	svc := s3.New(session.New())
	bucket := "dtenenba-test"
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			GGworkflowID := rand.Intn(20)
			assayMaterialID := rand.Intn(15)
			// fmt.Print(".")
			molecularID := rand.Intn(17)
			objectName := uuid.New().String()
			tags := fmt.Sprintf("stage=raw&GGworkflowId=%d&assayMaterialID=%d&molecularID=%d",
				GGworkflowID, assayMaterialID, molecularID)

			input := &s3.PutObjectInput{
				Body:   aws.ReadSeekCloser(strings.NewReader("filetoupload")),
				Bucket: aws.String(bucket),
				Key:    aws.String(objectName),
				// ServerSideEncryption: aws.String("AES256"), // FIXME use this in HSE
				Tagging: aws.String(tags),
			}

			_, err := svc.PutObject(input)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					default:
						fmt.Println(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					fmt.Println(err.Error())
				}
				// return
			}

			fmt.Printf("Uploading %s with tags %s...\n", objectName, tags)

		}()
		wg.Wait()
	}
}

func deleteFile(fileToUpload string, rec CSVRecord, wg *sync.WaitGroup, svc s3.S3, ops *uint64) {
	defer wg.Done()
	obj := rec.s3Prefix + fileToUpload
	_, derr := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(rec.s3TransferBucket),
		Key:    aws.String(obj),
	})
	if derr != nil {
		panic(derr)
	}
	atomic.AddUint64(ops, 1)
}

func fileExistsInS3(fileName string, rec CSVRecord, svc s3.S3) bool {
	res, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(rec.s3TransferBucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(rec.s3Prefix + fileName),
	})
	if err != nil {
		panic(err)
	}
	// fmt.Println("fileExistsInS3 got result", res)
	return *res.KeyCount > 0
}

func uploadFile(fileToUpload string, rec CSVRecord, wg *sync.WaitGroup, svc s3.S3, ops *uint64, guard chan struct{}) {
	defer wg.Done()
	defer func() { <-guard }()
	if fileExistsInS3(fileToUpload, rec, svc) {
		fmt.Println(rec.s3Prefix+fileToUpload, "already exists in S3; not overwriting.")
		return
	}
	st, err := os.Lstat(rec.localDir)
	if err != nil {
		panic(err)
	}
	var fd *os.File
	if st.IsDir() {
		fd, err = os.Open(rec.localDir + fileToUpload)
	} else {
		fd, err = os.Open(rec.localDir)
	}
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	v := url.Values{}
	v.Set("stage", rec.stage)
	v.Set("assay_material_id", rec.assayMaterialID)
	v.Set("molecular_id", rec.molecularID)
	v.Set("omics_sample_name", rec.omicsSampleName)

	// FIXME add omicsSampleName here?
	input := &s3.PutObjectInput{
		Bucket:  aws.String(rec.s3TransferBucket),
		Key:     aws.String(rec.s3Prefix + fileToUpload),
		Body:    fd,
		Tagging: aws.String(v.Encode()),
	}
	_, perr := svc.PutObject(input)
	if perr != nil {
		if aerr, ok := perr.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(perr.Error())
		}
		// return
	}
	atomic.AddUint64(ops, 1)
	fmt.Printf("Uploading %s%s with tags %s...\n", rec.s3Prefix, fileToUpload, v.Encode())

}

//
// func tagFile(fileToUpload string, rec CSVRecord, wg *sync.WaitGroup, svc s3.S3, ops *uint64, guard chan struct{}) {
// 	defer wg.Done()
// 	defer func() { <-guard }()
// 	if fileExistsInS3(fileToUpload, rec, svc) {
// 		fmt.Println(rec.s3Prefix+fileToUpload, "already exists in S3; not overwriting.")
// 		return
// 	}
// 	st, err := os.Lstat(rec.localDir)
// 	if err != nil {
// 		panic(err)
// 	}
// 	var fd *os.File
// 	if st.IsDir() {
// 		fd, err = os.Open(rec.localDir + fileToUpload)
// 	} else {
// 		fd, err = os.Open(rec.localDir)
// 	}
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer fd.Close()
// 	v := url.Values{}
// 	v.Set("stage", rec.stage)
// 	v.Set("assay_material_id", rec.assayMaterialID)
// 	v.Set("molecular_id", rec.molecularID)
// 	v.Set("omics_sample_name", rec.omicsSampleName)
//
// 	// FIXME add omicsSampleName here?
// 	input := &s3.PutObjectInput{
// 		Bucket:  aws.String(rec.s3TransferBucket),
// 		Key:     aws.String(rec.s3Prefix + fileToUpload),
// 		Body:    fd,
// 		Tagging: aws.String(v.Encode()),
// 	}
// 	_, perr := svc.PutObject(input)
// 	if perr != nil {
// 		if aerr, ok := perr.(awserr.Error); ok {
// 			switch aerr.Code() {
// 			default:
// 				fmt.Println(aerr.Error())
// 			}
// 		} else {
// 			// Print the error, cast err to awserr.Error to get the Code and
// 			// Message from an error.
// 			fmt.Println(perr.Error())
// 		}
// 		// return
// 	}
// 	atomic.AddUint64(ops, 1)
// 	fmt.Printf("Uploading %s%s with tags %s...\n", rec.s3Prefix, fileToUpload, v.Encode())
//
// }

//

func handleRecord(record []string, wg *sync.WaitGroup, svc s3.S3, cmd string, ops *uint64, guard chan struct{}) {
	rec := CSVRecord{molecularID: record[3], assayMaterialID: record[4],
		s3TransferBucket: record[1], s3Prefix: record[2], localDir: record[0],
		stage: record[5], omicsSampleName: record[6],
		dataType: DataType(strings.TrimSpace(record[7]))}

	var file os.FileInfo

	var files []os.FileInfo

	var uploadSingleFile bool

	if cmd == "tag" {
		// first, see if prefix refers to an object. if so, we are only tagging that object.
		// otherwise we are tagging all eligible objects that start with prefix.

		// input := &s3.ListObjectsV2Input{
		// 	Bucket: aws.String(rec.s3TransferBucket),
		// 	// Delimiter: aws.String("/"),
		// 	Prefix: aws.String(rec.s3Prefix),
		// }
		// res, err := svc.ListObjectsV2(input)
		// if err != nil {
		// 	panic(err)
		// }
		// fmt.Println("# returned:", len(res.Contents))
		input := &s3.HeadObjectInput{
			Bucket: aws.String(rec.s3TransferBucket),
			Key:    aws.String(rec.s3Prefix),
		}
		out, err := svc.HeadObject(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					fmt.Println(aerr.Error())
				}
			} else {

				if !strings.HasSuffix(rec.s3Prefix, "/") {
					rec.s3Prefix = rec.s3Prefix + "/"
				}

				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
		}
		fmt.Println("input is", input)
		fmt.Println("result is", out)
		os.Exit(1)

	} else {
		f, err := os.Lstat(rec.localDir)
		if err != nil {
			panic(err)
		}

		if f.IsDir() {
			if !strings.HasSuffix(rec.localDir, "/") {
				rec.localDir = rec.localDir + "/"
			}

			files, err = ioutil.ReadDir(rec.localDir)
			if err != nil {
				panic(err)
			}
		} else {
			uploadSingleFile = true
			files = append(files, f)
		}

	}

	for _, file = range files {
		if file.IsDir() {
			continue
		}
		if rec.dataType == SequencingData {
			// fastq

			if (!uploadSingleFile) &&
				!(strings.HasSuffix(file.Name(), ".fastq") ||
					strings.HasSuffix(file.Name(), ".fastq.gz")) {
				continue
			}
		}

		// }

		switch cmd {
		case "upload":
			wg.Add(1)
			guard <- struct{}{}
			go uploadFile(file.Name(), rec, wg, svc, ops, guard)
		// case "tag":
		// 	wg.Add(1)
		// 	guard <- struct{}{}
		// 	go tagFile(rec, wg, svc, ops, guard)
		case "delete":
			deleteFile(file.Name(), rec, wg, svc, ops)
		case "count":
			atomic.AddUint64(ops, 1)
		}
	}
}

func main() {
	kingpin.CommandLine.Help = `
Please supply the name of a csv file containing the following a header line
like this:

seq_dir,s3transferbucket,s3_prefix,molecular_id,assay_material_id,stage,omics_sample_name,data_type

NOTE: This program will use all available cores. Be a good citizen and
'grabnode' so that this program does not disrupt others' work.
More information:
https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Grab%%20Commands.aspx

Full documentation is here:
https://github.com/FredHutch/s3tagcrawler/tree/feature/uploadAndTag

`
	kingpin.Parse()

	var ops uint64
	maxGoroutines := runtime.NumCPU() - 1
	guard := make(chan struct{}, maxGoroutines)

	cmd := "upload"
	// if len(os.Args) > 2 {
	// 	cmd = os.Args[2]
	// }
	if *tagOnly {
		cmd = "tag"
	}
	os.Setenv("AWS_REGION", "us-west-2") // FIXME
	svc := s3.New(session.New())

	fileHandle, err := os.Open(*csvFile)
	if err != nil {
		panic(err)
	}
	defer fileHandle.Close()
	r := csv.NewReader(fileHandle)
	var wg sync.WaitGroup
	for i := 0; ; i++ {
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Fatal(readErr)
		}
		if i == 0 { // skip header
			continue
		}
		handleRecord(record, &wg, *svc, cmd, &ops, guard)
	}
	wg.Wait()
	fmt.Println("Number of ops:", atomic.LoadUint64(&ops))
}
