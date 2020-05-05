import argparse
import sys
from os.path import commonprefix
from shutil import copyfileobj

from smart_open import open

from s3mothball.s3mothball import write_tar, validate_tar, delete_files, open_archived_file


def do_validate(args):
        print("Validating %s against %s" % (args.tar_path, args.manifest_path))
        validate_tar(args.manifest_path, args.tar_path, args.strip_prefix, progress_bar=args.progress_bar)


def do_delete(args):
    print("Deleting objects listed in %s" % args.manifest_path)
    if not args.force:
        buckets = delete_files(args.manifest_path, dry_run=True)
        for bucket, keys in buckets.items():
            print(" * To delete: %s items from s3://%s/%s" % (len(keys['keys']), bucket, commonprefix(keys['keys'])))
            if keys['mismatched']:
                print("   * WARNING: %s keys skipped because ETag doesn't match TarMD5" % len(keys['mismatched']))
            if input("Delete objects? [y/N] ").lower() != 'y':
                print("Canceled.")
                return
    buckets = delete_files(args.manifest_path, dry_run=False)
    for bucket, keys in buckets.items():
        print(" * Deleted %s items from s3://%s/%s" % (len(keys['deleted']), bucket, commonprefix(keys['keys'])))
        if keys['mismatched']:
            print("   * WARNING: %s keys skipped because ETag doesn't match TarMD5" % len(keys['mismatched']))
        if keys['errors']:
            print("   * WARNING: %s keys returned an error message (permissions issue or key not found)" % len(keys['errors']))


def archive_command(args, parser):
    print("Writing %s and %s" % (args.tar_path, args.manifest_path))
    write_tar(args.archive_url, args.manifest_path, args.tar_path, args.strip_prefix, progress_bar=args.progress_bar)
    if args.validate:
        do_validate(args)
    if args.delete:
        do_delete(args)


def validate_command(args, parser):
        do_validate(args)


def delete_command(args, parser):
    if args.validate:
        if not args.tar_path:
            parser.error("tar_path is required unless --no-validate is set.")
        do_validate(args)
    do_delete(args)


def extract_command(args, parser):
    with open_archived_file(args.manifest_path, args.tar_path, args.file_path) as f:
        if args.out:
            with open(args.out, 'wb') as out:
                copyfileobj(f, out)
        else:
            copyfileobj(f, sys.stdout.buffer)


def main():
    parser = argparse.ArgumentParser(description='Archive files on S3.')
    parser.add_argument('--no-progress', dest='progress_bar', action='store_false', help="Don't show progress bar when archiving and validating")
    parser.set_defaults(progress_bar=True)
    subparsers = parser.add_subparsers(help='Use s3mothball <command> --help for help')

    # archive
    create_parser = subparsers.add_parser('archive', help='Create a new tar archive and manifest.')
    create_parser.add_argument('archive_url', help='S3 prefix to archive, e.g. s3://bucket/prefix/')
    create_parser.add_argument('manifest_path', help='Path or S3 URL for output manifest file')
    create_parser.add_argument('tar_path', help='Path or S3 URL for output tar file')
    create_parser.add_argument('--strip-prefix', help='optional prefix to strip from inventory file when writing tar', default='')
    create_parser.add_argument('--no-validate', dest='validate', action='store_false', help="Don't validate tar against manifest after creating")
    create_parser.add_argument('--delete', dest='delete', action='store_true', help="Delete files from archive_url after archiving")
    create_parser.add_argument('--force', dest='force', action='store_true', help="Delete without asking")
    create_parser.set_defaults(func=archive_command, validate=True, delete=False, force=False)

    # validate
    create_parser = subparsers.add_parser('validate', help='Validate an existing tar archive and manifest.')
    create_parser.add_argument('manifest_path', help='Path or S3 URL for manifest file')
    create_parser.add_argument('tar_path', help='Path or S3 URL for tar file')
    create_parser.add_argument('--strip-prefix', help='optional prefix to strip from inventory file when writing tar', default='')
    create_parser.set_defaults(func=validate_command)

    # delete
    create_parser = subparsers.add_parser('delete', help='Delete original files listed in manifest.')
    create_parser.add_argument('manifest_path', help='Path or URL for output manifest file')
    create_parser.add_argument('tar_path', nargs='?', help='Path or URL for output tar file')
    create_parser.add_argument('--strip-prefix', help='optional prefix to strip from inventory file when writing tar; needed if validating', default='')
    create_parser.add_argument('--no-validate', dest='validate', action='store_false', help="Don't validate tar against manifest before deleting")
    create_parser.add_argument('--force', dest='force', action='store_true', help="Delete without asking")
    create_parser.set_defaults(func=delete_command, validate=True, force=False)

    # extract
    create_parser = subparsers.add_parser('extract', help='Extract a file from an archive.')
    create_parser.add_argument('manifest_path', help='Path or URL for manifest file')
    create_parser.add_argument('tar_path', help='Path or URL for tar file')
    create_parser.add_argument('file_path', help='URL of file to extract from manifest, e.g. s3://<Bucket>/<Key>')
    create_parser.add_argument('--out', help='optional output path; default stdout')
    create_parser.set_defaults(func=extract_command)

    args = parser.parse_args()
    args.func(args, parser)