#!/usr/bin/env python3

"""
change tag names back from camelCase to under_score
This script only needed to be run once, and has already been run.
Adding it to github for posterity.
"""

import csv
import sys

import boto3

def main():
    "do the work"
    if len(sys.argv) != 2:
        print("supply a csv file")
        sys.exit(1)
    s3 = boto3.client("s3") # pylint: disable=invalid-name
    with open(sys.argv[1]) as csvfile:
        reader = csv.DictReader(csvfile)
        for idx, row in enumerate(reader):
            if idx == 0:
                continue
            prefix = row['s3_prefix']
            bucket = row['s3transferbucket']
            if not prefix.endswith("/"):
                prefix += "/"

            resp = s3.list_objects(Bucket=bucket, Delimiter="/", Prefix=prefix, MaxKeys=999)
            if resp['IsTruncated']:
                print("NEED TO DEAL WITH PAGINATION")
                sys.exit(1)
            files = [x['Key'] for x in resp['Contents'] if \
               x['Key'].endswith(".fastq") or x['Key'].endswith(".fastq.gz")]
            for _file in files:
                print("Processing {} ...".format(_file))
                resp = s3.get_object_tagging(Bucket=bucket, Key=_file)
                if not 'TagSet' in resp:
                    print("{} is not tagged!".format(_file))
                    sys.exit(1)
                tags = resp['TagSet']
                for pair in tags:
                    if pair['Key'] == 'assayMaterialId':
                        pair['Key'] = 'assay_material_id'
                    elif pair['Key'] == 'molecularID':
                        pair['Key'] = 'molecular_id'
                    elif pair['Key'] == 'omicsSampleName':
                        pair['Key'] = 'omics_sample_name'
                print("new tags are")
                print(tags)
                resp = s3.put_object_tagging(Bucket=bucket, Key=_file,
                                             Tagging=dict(TagSet=tags))
                print("Changed tags, new version is {}".format(resp['VersionId']))






if __name__ == "__main__":
    main()
