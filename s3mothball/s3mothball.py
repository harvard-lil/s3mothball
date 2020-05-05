import hashlib
from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from tarfile import TarFile, TarInfo
from tempfile import TemporaryDirectory

from smart_open import open
import boto3
from smart_open.s3 import parse_uri
from tqdm import tqdm

from s3mothball.helpers import HashingFile, LoggingTarFile, make_parent_dir, TeeFile, threaded_queue, OffsetSizeFile, \
    write_dicts_to_csv, read_dicts_from_csv, list_objects, load_object, chunks
from s3mothball.settings import SPOOLED_FILE_SIZE


def write_tar(archive_url, manifest_path, tar_path, strip_prefix=None, progress_bar=False):
    """
        Write all objects from archive_url to tar_path.
        Write list of objects to manifest_path.
    """
    files_written = []

    # write tar
    make_parent_dir(tar_path)
    with open(tar_path, 'wb', ignore_ext=True) as tar_out, \
         LoggingTarFile.open(fileobj=tar_out, mode='w|') as tar, \
         TemporaryDirectory() as temp_dir:

        # get iterator of items to tar, loaded in background threads
        objects = list_objects(archive_url)
        items = threaded_queue(load_object, ((obj, temp_dir) for obj in objects))

        # tar each item
        for obj, response, body in tqdm(items, disable=not progress_bar):
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
                ('Size', response['ContentLength']),
                ('LastModifiedDate', response['LastModified'].isoformat()),
                ('ETag', response['ETag'].strip('"')),
                ('StorageClass', response.get('StorageClass', 'STANDARD')),
                ('VersionId', response.get('VersionId', '')),
                # ('Owner', obj.owner['DisplayName'] if obj.owner else ''),
                # tar fields
                ('TarMD5', body.hexdigest()),
                ('TarOffset', member.offset),
                ('TarDataOffset', member.offset_data),
                ('TarSize', member.size),
            )))
            if response['ContentLength'] != member.size:
                raise ValueError("Object size mismatch: %s" % obj.key)

    # write csv
    make_parent_dir(manifest_path)
    files_written.sort(key=lambda f: f['Key'])
    write_dicts_to_csv(manifest_path, files_written)


def validate_tar(manifest_path, tar_path, strip_prefix='', progress_bar=False):
    """
        Verify that all items listed in manifest_path can be read from tar_path, and all items in tar_path are listed
        in manifest_path, with matching hashes and file names.
    """
    csv_entries = sorted(read_dicts_from_csv(manifest_path), key=lambda r: int(r['TarOffset']))
    if not csv_entries:
        raise ValueError("No entries found in manifest file.")

    with open(tar_path, 'rb', ignore_ext=True) as f:
        tar_f, raw_f = TeeFile.tee(f)
        tar = TarFile.open(fileobj=tar_f, mode='r|', bufsize=SPOOLED_FILE_SIZE)
        raw_f.read(int(csv_entries[0]['TarOffset']))
        for tarinfo in tqdm(tar, disable=not progress_bar):
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
    """
        Delete all files listed in manifest_path. File hashes are required to match the etag listed in the manifest.

        Returns a dictionary of results by bucket, e.g.:

        {
            'bucket': {'keys': [], 'deleted': [], 'errors': [], 'mismatched': []}
        }

        keys: all keys listed in manifest
        deleted: keys successfully deleted
        errors: keys not deleted by S3 (e.g. permissions problem or key not found)
        mismatched: keys whose 'ETag' column does not match the 'TarMD5' column

        Limitations:
        * etags of actual files deleted are not checked prior to deletion.
        * multipart-uploaded files will have mismatching hashes and will not be deleted.
    """
    buckets = defaultdict(lambda: {'keys': [], 'deleted': [], 'errors': [], 'mismatched': []})
    for entry in read_dicts_from_csv(manifest_path):
        if entry['ETag'] != entry['TarMD5']:
            buckets[entry['Bucket']]['mismatched'].append(entry['Key'])
            continue
        buckets[entry['Bucket']]['keys'].append(entry['Key'])
    if not dry_run:
        for bucket, keys in buckets.items():
            for delete_batch in chunks(keys['keys'], 1000):
                response = boto3.resource('s3').Bucket(bucket).delete_objects(
                    Delete={
                        'Objects': [{'Key': k} for k in delete_batch],
                        'Quiet': False,
                    }
                )
                keys['deleted'].extend(o['Key'] for o in response['Deleted'])
                keys['errors'].extend(o['Key'] for o in response['Errors'])
    return buckets


@contextmanager
def open_archived_file(manifest_path, tar_path, file_path):
    """
        Load a single file from the given tar_path, with offsets looked up from manifest_path, and original bucket and
        key for the file given by file_path.
    """
    parsed = parse_uri(file_path)
    entry = next((r for r in read_dicts_from_csv(manifest_path) if r['Bucket'] == parsed['bucket_id'] and r['Key'] == parsed['key_id']), None)
    if not entry:
        raise FileNotFoundError
    with open(tar_path, 'rb') as f:
        yield OffsetSizeFile(f, int(entry['TarDataOffset']), int(entry['TarSize']))
