# Upload and Tag Tool

This is a tool that uploads files in `/fh/fast` to Amazon S3, and tags them
according to a CSV file that you provide to prepare the data for downstream processes such as use with Globus Genomics. There's also an option to (re-)tag existing files
in an Amazon S3 bucket (without uploading).


## What the program does

For each line in the CSV (not including the first line containing column names), `upload_and_tag` will upload file(s) at the specified path in `/fh/fast`
(or in Swift) to the specified S3 bucket and prefix, with the defined tags.  If a path to a directory is given, all the files in the directory will be uploaded if `data_type == 0`, or only those files ending in `.fastq` and `.fastq.gz` if `data_type == 1`.  If the path includes a filename, only that file will be uploaded. If a file with the same name already exists, the program will not overwrite
it, but will indicate in its output that the file already exists.

### Obtaining S3 Credentials

Before you begin, make sure you have obtained your S3 credentials
by running the `awscreds` script, as documented
[here](http://sciwiki.fredhutch.org/computing/access_overview/#getting-aws-s3-credentials).

### Obtaining Swift Credentials

If you are uploading (and not just tagging existing objects in S3) 
and any of the files you are uploading are located in Swift, you must
set the required environment variables. If your PI is Jane Doe, you would 
do this:

```
sw2account doe_j
eval $(swc auth)
```

This will place into the environment credentials that will be valid for
7 days. It adds the following environment variables:

* `OS_AUTH_URL`
* `OS_STORAGE_URL`
* `OS_AUTH_TOKEN`




## Using the tool

You must supply a CSV file with the following column headers in the first line:

* `seq_dir`: The full path to a directory in `/fh/fast` that contains files to be
  uploaded to S3, *or* the full path including file name to a single file to upload.
  If you are uploading from Swift instead of `/fh/fast`, the value
  of `seq_dir` should start with `swift://` and take the following form: `swift://containername/path/to/file/or/dir`.
  If `seq_dir` refers to a directory, only files in the top level of the directory
  will be uploaded. **NOTE:** All matching file(s) at `seq_dir` will be tagged with the same tags, thus this is intended to be given the path to a directory containing data files to be used as a group.  An example is all the fastq's made from a sequencing run for a given sample, thus all the file names are likely *sample1_TGACCA_L001_R1_001.fastq.gz*, *sample1_TGACCA_L001_R2_001.fastq.gz*, etc but the directory contains an arbitrary number of files.  If you are tagging without uploading, leave this column blank.
* `s3transferbucket`: The name of the S3 bucket to upload to. Should
  not have an `s3://` prefix (e.g., just unquoted "fh-pi-paguirigan-a"). You must have write access to this bucket and credentials saved in your ~/.aws directory in order
  to use this tool.
* `s3_prefix`: The prefix in S3 where the file(s) in `fast_path` should be uploaded.
* `data_type`: either `1` or `0`, defines whether *only* `.fastq` and `.fastq.gz` files in `seq_dir` are uploaded (if `data_type == 1`), or if *all* files in `seq_dir` are uploaded (if `data_type == 0`).  Suggested use is `1` for sequencing data where only the fastq's are of interest to transfer and `0` for other data sets such as raw array data, processed custom data sets that are not fastq files but the directory contains only files intended to be analyzed as a set.

Additionally you must supply at least one additional column 
for tagging. 

Here's an example CSV file:

```csv
seq_dir,s3transferbucket,s3_prefix.data_type,color,month
/fh/fast/doe_j/some_files,fh-pi-doe-j,some_files,1,blue,september
/fh/fast/doe_j/some_other_files,fh-pi-doe-j,some_other_files,0,purple,november
/fh/fast/doe_j/path_to/a_file.txt,fh-pi-doe-j,lonely_files,0,red,may
swift://user_jbrown/path/to/files,fh-pi-doe-j,files-from-swift,0,mauve,april
```

For ease of reading, here's the same CSV as a table:



| seq_dir  | s3transferbucket   | s3_prefix  | data_type  | color  | month  |
|---|---|---|---|---|---|
| /fh/fast/doe_j/some_files   | fh-pi-doe-j   | some_files   | 1  | blue  | september   |
| /fh/fast/doe_j/some_other_files  | fh-pi-doe-j   | some_other_files   | 0  | purple  | november  |
|/fh/fast/doe_j/path_to/a_file.txt|fh-pi-doe-j|lonely_files|0|red|may
|swift://user_jbrown/path/to/files|fh-pi-doe-j|files-from-swift|0|mauve|april|
|


This will upload any `.fastq` or `.fastq.gz` files found
in `/fh/fast/doe_j/some_files` to the `fh-pi-doe-j` bucket
under the prefix `some_files/`, and tag them with the key-value
pairs `color=blue` and `month=september`. 
It will also upload *all* files found in `/fh/fast/doe_j/some_other_files` to the same bucket, under the
prefix `some_other_files`, and tag them with the key-value pairs
`color=purple` and `month=november`.
It will upload the single file `/fh/fast/doe_j/path_to/a_file.txt` to the same bucket, under the prefix `lonely_files`, and tag it with the key-value pairs
`color=red` and `month=may`.
Finally, it will upload all objects in the Swift container `user_jbrown` 
whose path starts with `path/to/files` to the same bucket under the prefix 
`files-from-swift` and tag it with the key-value pairs
`color=mauve` and `month=april`.

You are free to add as many columns/tags as you need.
By convention, the following tags have been used so far:

* `molecular_id`:   Suggested use is to maintain a list/database of sets of raw molecular data sets with unique identifiers for your work.  This list/database should also include metadata about the dataset itself (such as if it's RNA Seq, what library prep type was done, read length, paired end?, etc).  
* `assay_material_id`:  Suggested use is to maintain a list/database of assay materials (such as RNA or DNA from specimens), that were used to generate molecular data sets. This list/database should also include metadata about the assay material itself (such as whether the sample was a control or exposed condition, type of RNA extraction done, RIN or other QC metrics, etc).  
* `stage`:  Should be `raw` for raw data or `processed` for processed data. In this instance, an example use of this tag is the de-multiplexed fastq data for a sequencing run serves as the `raw` data for a bioinformatic process that generates a vcf or gene-count list which would be the `processed` data set.  
* `omics_sample_name`: This tag typically will be the string used to name the files generated, and in the case of the example sequencing data *sample1_TGACCA_L001_R1_001.fastq.gz*, the `omics_sample_name` is "sample1", and is the string that would be grep'd for to find all associated files for that sample.

**IMPORTANT NOTE**: This program is designed to use every available CPU core
and upload a file on each core. **Do not run this program on the rhino
machines or you will get yelled at.** Instead, use
[grabnode](https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Grab%20Commands.aspx).

Once you have prepared the csv file, you can invoke the program as follows:

```
grabnode # request a min of 4 cores, 8GB of memory for 1 day
upload_and_tag name_of_your_file.csv
exit # relinquishes the node you grabbed, otherwise it will be yours for the entire day!
```

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
