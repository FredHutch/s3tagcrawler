# Upload and Tag Tool

This is a tool that uploads files in `/fh/fast` to Amazon S3, and tags them
according to a CSV file that you provide.

**IMPORTANT NOTE**: This program is designed to use every available CPU core
and upload a file on each core. **Do not run this program on the rhino
machines or you will get yelled at.** Instead, use
[grabnode](https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Grab%20Commands.aspx).

Specify that you want at least 16 cores and at least 8GB of memory.

## Using the tool

You must supply a CSV file which has a header line like this:

```
seq_dir,s3transferbucket,s3_prefix,molecular_id,assay_material_id,stage,omics_sample_name,data_type
```

Where each column is as follows (column order is important!):

* `seq_dir`: The full path to a directory in `/fh/fast` that contains files to be
  uploaded to S3, **or** the path to a single file to upload.
  If a directory, only files in the top level of the directory
  will be uploaded.
* `s3transferbucket`: The name of the S3 bucket to upload to. Should
  not have an `s3://` prefix. You must have write access to this bucket in order
  to use this tool.
* `s3_prefix`: The prefix in S3 where the files in `seq_dir` should be uploaded.
* `molecular_id`: Value for the `molecular_id` tag.
* `assay_material_id`: Value for the `assay_material_id` tag.
* `stage`: Value for the `stage` tag. Should be `raw` for raw data.
* `omics_sample_name`: Value for the `omics_sample_name` tag.
* `data_type`: either 1 or 0. 1 for sequencing data and 0 for array data. The
  only difference between the two is that if `seq_dir` is a directory, and
  `data_type` is 1, the program will upload *only* `.fastq` and `.fastq.gz` files.
  If `data_type` is 0, the program will upload *all* files in `seq_dir`.

Note that the actual names in the header row are not important to the program.
It uses column positions and not the values of the header column. In other words,
it expects the local directory to be the first column, the s3 bucket to be the second,
and so on.


Once you have prepared the csv file, you can invoke the program as follows:

```
grabnode # if you haven't already
upload_and_tag name_of_your_file.csv
exit # relinquishes the node you grabbed
```

## What the program does

For each line in the CSV, `upload_and_tag` will upload all `*.fastq` and
`*.fastq.gz` files to the specified bucket and prefix, with the appropriate tags.
If a file with the same name already exists, the program will not overwrite
it, but will indicate in its output that the file already exists.

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

## Problems and Support

If the program does not work as it should, please
[file an issue](https://github.com/FredHutch/s3tagcrawler/issues/new)
or email [scicomp@fredhutch.org](mailto:scicomp@fredhutch.org).
