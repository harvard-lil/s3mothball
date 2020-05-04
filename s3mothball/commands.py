import argparse
import sys
from shutil import copyfileobj

from smart_open import open

from s3mothball.s3mothball import write_tar, validate_tar, delete_files, open_archived_file


def archive_command(args, parser):
    write_tar(args.archive_url, args.manifest_path, args.tar_path, args.strip_prefix)
    if args.validate:
        validate_tar(args.manifest_path, args.tar_path, args.strip_prefix)
    if args.delete:
        delete_files(args.manifest_path, dry_run=False)


def validate_command(args, parser):
    validate_tar(args.manifest_path, args.tar_path, args.strip_prefix)


def delete_command(args, parser):
    if args.validate:
        if not args.tar_path:
            parser.error("tar_path is required unless --no-validate is set.")
        validate_tar(args.manifest_path, args.tar_path, args.strip_prefix)
    delete_files(args.manifest_path, dry_run=args.dry_run)


def extract_command(args, parser):
    with open_archived_file(args.manifest_path, args.tar_path, args.file_path) as f:
        if args.out:
            with open(args.out, 'wb') as out:
                copyfileobj(f, out)
        else:
            copyfileobj(f, sys.stdout.buffer)


def main():
    parser = argparse.ArgumentParser(description='Archive files on S3.')
    subparsers = parser.add_subparsers(help='Use s3mothball <command> --help for help')

    # set up s3mothball create
    create_parser = subparsers.add_parser('archive', help='Create a new tar archive and manifest.')
    create_parser.add_argument('archive_url', help='S3 prefix to archive, e.g. s3://bucket/prefix/')
    create_parser.add_argument('manifest_path', help='Path or S3 URL for output manifest file')
    create_parser.add_argument('tar_path', help='Path or S3 URL for output tar file')
    create_parser.add_argument('--strip-prefix', help='optional prefix to strip from inventory file when writing tar', default='')
    create_parser.add_argument('--no-validate', dest='validate', action='store_false', help="Don't validate tar against manifest after creating")
    create_parser.add_argument('--delete', dest='delete', action='store_true', help="Delete files from archive_url after archiving")
    create_parser.set_defaults(func=archive_command, validate=True, delete=False)

    # set up s3mothball validate
    create_parser = subparsers.add_parser('validate', help='Validate an existing tar archive and manifest.')
    create_parser.add_argument('manifest_path', help='Path or S3 URL for manifest file')
    create_parser.add_argument('tar_path', help='Path or S3 URL for tar file')
    create_parser.add_argument('--strip-prefix', help='optional prefix to strip from inventory file when writing tar', default='')
    create_parser.set_defaults(func=validate_command)

    # set up s3mothball delete
    create_parser = subparsers.add_parser('delete', help='Delete original files listed in manifest.')
    create_parser.add_argument('manifest_path', help='Path or URL for output manifest file')
    create_parser.add_argument('tar_path', nargs='?', help='Path or URL for output tar file')
    create_parser.add_argument('--strip-prefix', help='optional prefix to strip from inventory file when writing tar; needed if validating', default='')
    create_parser.add_argument('--no-validate', dest='validate', action='store_false', help="Don't validate tar against manifest before deleting")
    create_parser.add_argument('--no-dry-run', dest='dry_run', action='store_false', help="Actually delete instead of doing a dry run")
    create_parser.set_defaults(func=delete_command, validate=True, dry_run=True)

    # set up s3mothball extract
    create_parser = subparsers.add_parser('extract', help='Extract a file from an archive.')
    create_parser.add_argument('manifest_path', help='Path or URL for manifest file')
    create_parser.add_argument('tar_path', help='Path or URL for tar file')
    create_parser.add_argument('file_path', help='URL of file to extract from manifest, e.g. s3://<Bucket>/<Key>')
    create_parser.add_argument('--out', help='optional output path; default stdout')
    create_parser.set_defaults(func=extract_command)

    args = parser.parse_args()
    args.func(args, parser)