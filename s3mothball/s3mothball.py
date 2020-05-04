import csv
import hashlib
from collections import OrderedDict
from contextlib import contextmanager
from tarfile import TarFile, TarInfo
from tempfile import SpooledTemporaryFile, TemporaryDirectory

from smart_open import open
import boto3
from smart_open.s3 import parse_uri
from tqdm import tqdm

from s3mothball.helpers import HashingFile, LoggingTarFile, make_parent_dir, TeeFile, threaded_queue, OffsetSizeFile

# TODO:
# check API call count
# check modification timezone
# validation
# deletion

# boto3.set_stream_logger('botocore.endpoint', logging.DEBUG)
SPOOLED_FILE_SIZE = 10 * 2**20
THREADS = 8  # 8 seems to be enough; has to be enough to load items from S3 faster than a thread can tar them


def list_objects(s3_url):
    source_path_parsed = parse_uri(s3_url)
    bucket = boto3.resource('s3').Bucket(source_path_parsed['bucket_id'])
    return bucket.objects.filter(Prefix=source_path_parsed['key_id'].rstrip('/') + '/')


def load_object(obj, temp_dir):
    response = obj.get()
    body = SpooledTemporaryFile(SPOOLED_FILE_SIZE, dir=temp_dir)
    body.write(response['Body'].read())
    body.seek(0)
    return obj, response, body


def write_tar(archive_url, manifest_path, tar_path, strip_prefix=None):
    files_written = []

    # write tar
    print("Writing %s" % tar_path)
    make_parent_dir(tar_path)
    with \
            open(tar_path, 'wb', ignore_ext=True) as tar_out, \
            LoggingTarFile.open(fileobj=tar_out, mode='w|') as tar, \
            TemporaryDirectory() as temp_dir \
        :

        # get iterator of items to tar
        objects = list_objects(archive_url)
        items = threaded_queue(load_object, ((obj, temp_dir) for obj in objects), THREADS)

        for obj, response, body in tqdm(items):
            body = HashingFile(body)
            tar_info = TarInfo()
            tar_info.size = int(response['ContentLength'])
            tar_info.mtime = response['LastModified'].timestamp()
            tar_info.name = obj.key
            if strip_prefix and tar_info.name.startswith(strip_prefix):
                tar_info.name = tar_info.name[len(strip_prefix):]
            tar.addfile(tar_info, body)
            member = tar.members[-1]
            files_written.append(OrderedDict((
                # inventory fields
                ('Bucket', obj.bucket_name),
                ('Key', obj.key),
                ('Size', obj.size),
                ('LastModifiedDate', response['LastModified'].isoformat()),
                ('ETag', obj.e_tag.strip('"')),
                ('StorageClass', obj.storage_class or 'STANDARD'),
                ('VersionId', response.get('VersionId', '')),
                # ('Owner', obj.owner['DisplayName'] if obj.owner else ''),
                # tar fields
                ('TarMD5', body.hexdigest()),
                ('TarOffset', member.offset),
                ('TarDataOffset', member.offset_data),
                ('TarSize', member.size),
            )))
            if obj.size != member.size:
                raise ValueError("Object size mismatch: %s" % obj.key)

    # write csv
    print("Writing %s" % manifest_path)
    make_parent_dir(manifest_path)
    files_written.sort(key=lambda f: f['Key'])
    with open(manifest_path, 'w', newline='') as out:
        writer = csv.DictWriter(out, fieldnames=list(files_written[0].keys()))
        writer.writeheader()
        writer.writerows(files_written)


def validate_tar(manifest_path, tar_path, strip_prefix=''):
    print("Verifying %s against %s" % (tar_path, manifest_path))
    with open(manifest_path, newline='') as f:
        csv_entries = sorted(csv.DictReader(f), key=lambda r: int(r['TarOffset']))
    if not csv_entries:
        raise ValueError("No entries found in manifest file.")

    with open(tar_path, 'rb', ignore_ext=True) as f:
        tar_f, raw_f = TeeFile.tee(f)
        tar = TarFile.open(fileobj=tar_f, mode='r|', bufsize=SPOOLED_FILE_SIZE)
        raw_f.read(int(csv_entries[0]['TarOffset']))
        for tarinfo in tqdm(tar):
            if not csv_entries:
                raise ValueError("Not enough files found in manifest. Looking for: %s" % tarinfo.name)
            csv_entry = csv_entries.pop(0)
            if tarinfo.name != csv_entry['Key'][len(strip_prefix):]:
                raise ValueError("Mismatched keys: tar has %s, manifest has %s" % (tarinfo.name, csv_entry['Key'][len(strip_prefix):]))
            if tarinfo.offset != int(csv_entry['TarOffset']):
                raise ValueError("Tar file offset mismatch: %s" % tarinfo.name)
            if tarinfo.offset_data != int(csv_entry['TarDataOffset']):
                raise ValueError("Tar file data offset mismatch: %s" % tarinfo.name)
            if tarinfo.size != int(csv_entry['TarSize']):
                raise ValueError("Tar file size mismatch: %s" % tarinfo.name)
            tar_contents = tar.extractfile(tarinfo)
            size = tarinfo.size
            raw_f.read(int(csv_entry['TarDataOffset']) - raw_f.tell())
            checksum = hashlib.md5()
            while size > 0:
                read_len = min(size, SPOOLED_FILE_SIZE)
                chunk1 = tar_contents.read(read_len)
                chunk2 = raw_f.read(read_len)
                if chunk1 != chunk2:
                    raise ValueError("File content mismatch: %s" % tarinfo.name)
                checksum.update(chunk1)
                size -= read_len
            if checksum.hexdigest() != csv_entry['TarMD5']:
                raise ValueError("File hash mismatch: %s" % tarinfo.name)

    if csv_entries:
        raise ValueError("Manifest files not found in tar: %s" % ", ".join(c['Key'] for c in csv_entries))


def delete_files(manifest_path, dry_run=True):
    print("Deleting files from %s" % manifest_path)

    buckets = {}
    with open(manifest_path, newline='') as f:
        for entry in csv.DictReader(f):
            if entry['ETag'] != entry['TarMD5']:
                print("Mismatched ETag and TarMD5 for %s, skipping" % entry['Key'])
                continue
            buckets.setdefault(entry['Bucket'], []).append(entry['Key'])

    for bucket, keys in buckets.items():
        if dry_run:
            print("Would delete from %s:" % bucket)
            for k in keys:
                print(" * %s" % k)
        else:
            boto3.resource('s3').Bucket(bucket).delete_keys(keys, quiet=True)


@contextmanager
def open_archived_file(manifest_path, tar_path, file_path):
    parsed = parse_uri(file_path)
    with open(manifest_path, newline='') as f:
        entry = next((r for r in csv.DictReader(f) if r['Bucket'] == parsed['bucket_id'] and r['Key'] == parsed['key_id']), None)
    if not entry:
        raise FileNotFoundError
    with open(tar_path, 'rb') as f:
        yield OffsetSizeFile(f, int(entry['TarDataOffset']), int(entry['TarSize']))
