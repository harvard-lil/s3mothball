# s3mothball

s3mothball is an archival tool to:

* bundle up directories of small files on AWS S3 into tar archives indexed by csv files;
* validate those archives;
* delete the original small files; and
* extract original files from the archives via range requests.

## Installation

    pip install https://github.com/harvard-lil/s3mothball/archive/master.zip#egg=s3mothball
    
## System requirements

s3mothball requires Python 3.

## Why s3mothball?

When working with smaller files in S3 Glacier the costs of depositing, retrieving, and tagging 
files are often larger than the cost of storage itself.

For example, as of May, 2020, storing 1TB on S3 Glacier Deep Archive costs $1/month. But if that 1TB
consists of one million 1MB files, it will cost:

* $50 (four years' storage cost) to transition the million objects from standard storage
* $25 (two years' storage cost) to transition the million objects back to standard storage
* $1/month (doubling monthly storage cost) to keep one tag on each file

s3mothball helps by bundling a directory of s3 files into a single tar file, along with an index that
allows for listing and retrieving individual files.

## Usage

    $ s3mothball --help
    usage: s3mothball [-h] {archive,validate,delete,extract} ...
    
    Archive files on S3.
    
    positional arguments:
      {archive,validate,delete,extract}
                            Use s3mothball <command> --help for help
        archive             Create a new tar archive and manifest.
        validate            Validate an existing tar archive and manifest.
        delete              Delete original files listed in manifest.
        extract             Extract a file from an archive
    
    optional arguments:
      -h, --help            show this help message and exit
      
## Example

Suppose you have a set of files in S3 like this:

    my-bucket:
        my-files/
            0001.xml
            0002.xml
            ...
            1000.xml 

You could bundle them into a single archive like this:

    $ s3mothball archive s3://my-bucket/my-files/ s3://my-attic/manifests/my-bucket/my-files.tar.csv s3://my-attic/files/my-bucket/my-files.tar 

This command will write two files:

* s3://my-attic/files/my-bucket/my-files.tar is a tar file of all files in s3://my-bucket/my-files/
* s3://my-attic/manifests/my-bucket/my-files.tar.csv is a csv in this format:
        ```
        Bucket,Key,Size,LastModifiedDate,ETag,StorageClass,VersionId,TarMD5,TarOffset,TarDataOffset,TarSize
        my-bucket,my-files/0001.xml,817266,2018-11-17T22:06:08+00:00,4d0d142abe2bddc1a4021eea9a7f8620,STANDARD,bgmqNWTD4v4nWh.5iOCTIsxSAUYMq2VC,4d0d142abe2bddc1a4021eea9a7f8620,1024,1536,817266
        ```
    This emulated the format of an S3 inventory report, plus some tar-specific columns.

By default, `$ s3mothball archive` will fetch the files back from S3 and verify that all file names and contents
match between the tar file and manifest (you can prevent this with `--no-validate`).
You can also perform the same validation later:

    $ s3mothball validate s3://my-attic/manifests/my-bucket/my-files.tar.csv s3://my-attic/files/my-bucket/my-files.tar

Once you are satisfied with the archived version, you can delete the original files. By default delete will
print what files would be deleted but will not actually delete them:

    $ s3mothball delete s3://my-attic/manifests/my-bucket/my-files.tar.csv
    Deleting objects listed in s3://my-attic/manifests/my-bucket/my-files.tar.csv
     * To delete: 1000 items from s3://my-bucket/my-files
    Delete objects? [y/N] y
     * Deleted 1000 items from s3://my-bucket/my-files

If you later want to fetch an individual file like `s3://my-bucket/my-files/0001.xml`, you can do so with `extract`:

    $ s3mothball extract s3://my-attic/manifests/my-bucket/my-files.tar.csv \
        s3://my-attic/files/my-bucket/my-files.tar \
        s3://my-bucket/my-files/0001.xml \
        > 0001.xml

You would likely set up lifecycle rules to transition files in s3://my-attic/files/ to Glacier storage.
`$ s3mothball extract` would then require you to retrieve a particular tar file prior to extraction, or at least the
range within that tar referred to by the manifest.

## Path formats

s3mothball uses the smart_open library for tar and csv paths. This means that a wide variety of urls and compression
formats will work for the csv and tar paths. For example:

    $ s3mothball archive s3://my-bucket/my-files/ my-files.csv.gz my-files.tar
    
would write the manifest to a local, gzipped csv. See [smart_open](https://pypi.org/project/smart-open/) for a
complete list of supported URL formats.

The tar path does not currently support compression (`my-files.tar.gz` would not work), though in principle it could.

## Resource requirements

s3mothball attempts to be efficient with time, disk, RAM, and API usage. It should have this performance when archiving:

* Speed limited by the speed Python can write consecutive files to tar.
* Constant RAM usage regardless of number of objects archived (less than 200MB in one test).
* Constant disk usage regardless of number of objects archived, if .tar is streamed back to S3. Because fetch is
  multithreaded, max disk usage is the size of 8 of the objects being archived. This disk usage could in principle
  be avoided at the cost of slower archiving.
* Minimal S3 API queries -- one ListObjects per thousand files archived, and one GetObject per file archived. 
