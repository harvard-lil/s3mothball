# s3mothball

s3mothball is an archival tool to:

* bundle up directories of small files on AWS S3 into tar archives indexed by csv files;
* validate those archives;
* delete the original small files; and
* extract original files from the archives via range requests.

## Installation

    pip install https://github.com/harvard-lil/s3mothball/archive/master.zip#egg=s3mothball
    
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