# Upload and Tag Tool

This is a tool that uploads files in `/fh/fast` to Amazon S3, and tags them
according to a CSV file that you provide to prepare the data for downstream processes such as use with Globus Genomics. There's also an option to (re-)tag existing files
in an Amazon S3 bucket (without uploading).

**IMPORTANT NOTE**: This program is designed to use every available CPU core
and upload a file on each core. **Do not run this program on the rhino
machines or you will get yelled at.** Instead, use
[grabnode](https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Grab%20Commands.aspx).

Specify that you want at least 4 cores and at least 8GB of memory.

## Using the tool

You must supply a CSV file which has a header line like this:

```
fast_path, s3_transferbucket, s3_prefix, molecular_id, assay_material_id, stage, omics_sample_name, data_type
```
**Note:** The actual names in the header row that are not tags (e.g. `fast_path`), are not important to the program.
It uses column positions and not the values of the header column. In other words,
it expects the local directory to be the first column, the s3 bucket to be the second,
and so on.

* `fast_path`: The full path to a directory in `/fh/fast` that contains files to be
  uploaded to S3, *or* the full path including file name to a single file to upload.
  If fast_path refers to a directory, only files in the top level of the directory
  will be uploaded. **NOTE:** All matching file(s) at `fast_path` will be tagged with the same tags, thus this is intended to be given the path to a directory containing data files to be used as a group.  An example is all the fastq's made from a sequencing run for a given sample, thus all the file names are likely *sample1_TGACCA_L001_R1_001.fastq.gz*, *sample1_TGACCA_L001_R2_001.fastq.gz*, etc but the directory contains an arbitrary number of files.  If you are tagging without uploading, leave this column blank. 
* `s3_transferbucket`: The name of the S3 bucket to upload to. Should
  not have an `s3://` prefix (e.g., just unquoted "fh-pi-paguirigan-a"). You must have write access to this bucket and credentials saved in your ~/.aws directory in order
  to use this tool. 
* `s3_prefix`: The prefix in S3 where the file(s) in `fast_path` should be uploaded.
* `molecular_id`: Value for the `molecular_id` tag.
* `assay_material_id`: Value for the `assay_material_id` tag.
* `stage`: Value for the `stage` tag. Should be `raw` for raw data or `processed` for processed data.
* `omics_sample_name`: Value for the `omics_sample_name` tag.
* `data_type`: either 1 or 0, defines whether *only* `.fastq` and `.fastq.gz` files in `fast_path` are uploaded (if `fast_path == 1`), or if *all* files in `fast_path` are uploaded (if `fast_path == 0`).  Suggested use is 1 for sequencing data where only the fastq's are of interest to transfer and 0 for other data sets such as raw array data, processed custom data sets that are not fastq files but the directory contains only files intended to be analyzed as a set. 



Once you have prepared the csv file, you can invoke the program as follows:

```
grabnode # if you haven't already
upload_and_tag name_of_your_file.csv
exit # relinquishes the node you grabbed
```

## What the program does

For each line in the CSV, `upload_and_tag` will upload file(s) at the specified path in `/fh/fast` to the specified S3 bucket and prefix, with the defined tags.  If a path to a directory is given, all the files in the directory will be uploaded if `data_type == 0`, or only those files ending in `.fastq` and `.fastq.gz` if `data_type == 1`.  If the path includes a filename, only that file will be uploaded. If a file with the same name already exists, the program will not overwrite
it, but will indicate in its output that the file already exists. 

## To tag only (without uploading)

Use the `-t` (or `--tag-only`) option. Adding this option will cause the program
to do the following, for each row in the input CSV file:

* Get a list of all objects at the specified prefix (if `data_type` is 1,
  only `*.fastq` and `*.fastq.gz` files are included for tagging).
* Tags each of these objects with the tags specified in the current row.
* If the prefix specified does not exist in S3, nothing is done.  

## How to verify that a file has the correct tags:

You can issue a command like the following:

```
aws s3api get-object-tagging --bucket fh-pi-paguirigan-a --key \
CARDINAL/seq/fastq/Sample_138062561-CARD6602/138062561-CARD6602_R2_1.fastq.gz
```

It will return something like this:

```json
{
    "VersionId": ".sDUbn_uci.9Rooid6E4pum6FDkYqDby",
    "TagSet": [
        {
            "Value": "R0625",
            "Key": "assay_material_id"
        },
        {
            "Value": "raw",
            "Key": "stage"
        },
        {
            "Value": "M00000805",
            "Key": "molecular_id"
        },
        {
            "Value": "138062561-CARD6602",
            "Key": "omics_sample_name"
        }
    ]
}  
```

Also see the  [get-s3-tags](https://github.com/FredHutch/get-s3-tags) tool
which can return tags for all (or some) items in a bucket.


## Problems and Support

If the program does not work as it should, please
[file an issue](https://github.com/FredHutch/s3tagcrawler/issues/new)
or email [scicomp@fredhutch.org](mailto:scicomp@fredhutch.org).
