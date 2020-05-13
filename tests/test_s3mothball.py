import csv
import tarfile

import pytest
from smart_open import open

from s3mothball.helpers import write_dicts_to_csv, read_dicts_from_csv, list_objects
from tests.helpers import write_file


def test_write_tar(s3, files, source_bucket, archive_url, manifest_path, tar_path, boto_calls):
    from s3mothball.s3mothball import write_tar  # ensure mock is in place before importing functions to test

    # write tar
    strip_prefix = 'folders/'
    write_tar(archive_url, manifest_path, tar_path, strip_prefix=strip_prefix)

    # no unnecessary boto calls
    assert boto_calls == {
        'CompleteMultipartUpload': 2,
        'CreateMultipartUpload': 2,
        'GetObject': 2,
        'ListObjects': 1,
        'UploadPart': 2
    }

    # check tar file
    files_by_key = {e['key'][len(strip_prefix):]: e for e in files}
    with open(tar_path, 'rb', ignore_ext=True) as f:
        tar = tarfile.TarFile.open(fileobj=f, mode='r|')
        for member in tar:
            assert member.name in files_by_key
            entry = files_by_key[member.name]
            assert tar.extractfile(member).read() == entry['contents']
            entry.update({'offset': member.offset, 'offset_data': member.offset_data})
            del files_by_key[member.name]
    assert not files_by_key

    # check manifest
    with open(manifest_path, newline='') as f:
        expected_entries = [
            {
                "Bucket": source_bucket,
                "Key": entry['key'],
                "LastModifiedDate": entry['modified'].isoformat(),
                "Size": str(entry['size']),
                "ETag": entry['etag'],
                "StorageClass": 'STANDARD',
                "VersionId": '',  # not set by moto
                "TarMD5": entry['etag'],
                "TarOffset": str(entry['offset']),
                "TarDataOffset": str(entry['offset_data']),
                "TarSize": str(entry['size']),
                "TarStrippedPrefix": strip_prefix,
            }
            for entry in files
        ]
        entries = list(csv.DictReader(f))
        assert entries == expected_entries


def test_validate_tar(s3, files, source_bucket, archive_url, manifest_path, tar_path, boto_calls):
    from s3mothball.s3mothball import write_tar, validate_tar  # ensure mock is in place before importing functions to test

    # write tar
    strip_prefix = 'folders/'
    write_tar(archive_url, manifest_path, tar_path, strip_prefix=strip_prefix)

    boto_calls.clear()

    # validates successfully
    validate_tar(manifest_path, tar_path)

    # no unnecessary boto calls
    # this should be only 2 calls -- see https://github.com/RaRe-Technologies/smart_open/issues/494
    assert boto_calls == {'GetObject': 4}

    # load contents
    manifest = list(read_dicts_from_csv(manifest_path))
    with open(tar_path, 'rb', ignore_ext=True) as f:
        tar_contents = f.read()

    # manifest missing a line
    write_dicts_to_csv(manifest_path, manifest[:-1])
    with pytest.raises(ValueError, match=r"Not enough files found in manifest"):
        validate_tar(manifest_path, tar_path)

    # manifest has extra line
    write_dicts_to_csv(manifest_path, manifest + [manifest[-1]])
    with pytest.raises(ValueError, match=r"Manifest files not found in tar"):
        validate_tar(manifest_path, tar_path)

    # manifest with wrong hashes
    write_dicts_to_csv(manifest_path, [{**m, 'TarMD5': 'foo'} for m in manifest])
    with pytest.raises(ValueError, match=r"File hash mismatch"):
        validate_tar(manifest_path, tar_path)

    # manifest with wrong names
    write_dicts_to_csv(manifest_path, [{**m, 'Key': m['Key'] + 'abc'} for m in manifest])
    with pytest.raises(ValueError, match=r"Mismatched keys"):
        validate_tar(manifest_path, tar_path)

    # detect tar corrupted
    with open(tar_path, 'wb', ignore_ext=True) as f:
        f.write(b'ABCD'+tar_contents)
    with pytest.raises(tarfile.ReadError):
        validate_tar(manifest_path, tar_path)


def test_delete_files(s3, files, source_bucket, archive_url, manifest_path, tar_path, boto_calls):
    from s3mothball.s3mothball import delete_files, write_tar  # ensure mock is in place before importing functions to test

    # write tar
    strip_prefix = 'folders/'
    write_tar(archive_url, manifest_path, tar_path, strip_prefix=strip_prefix)

    # write extra files
    mismatched_etag_key = 'folders/some_folder/mismatched.txt'
    deleted_etag_key = 'folders/some_folder/deleted.txt'
    keys_to_keep = {'folders/some_folder/keep.txt', mismatched_etag_key}
    keys_to_delete = set(f['key'] for f in files)
    for key in keys_to_keep:
        write_file(s3, source_bucket, key, 'contents')

    # write mismatched file to manifest
    manifest = list(read_dicts_from_csv(manifest_path))
    write_dicts_to_csv(manifest_path, manifest + [
        {
            **manifest[-1],
            'Key': mismatched_etag_key,
            'TarMD5': 'mismatched-md5',
        },
        {
            **manifest[-1],
            'Key': deleted_etag_key,
        },
    ])

    # dry run delete
    boto_calls.clear()
    buckets = delete_files(manifest_path)
    assert buckets[source_bucket]['deleted'] == []
    assert buckets[source_bucket]['errors'] == []
    assert set(buckets[source_bucket]['keys']) == keys_to_delete | {deleted_etag_key}
    assert buckets[source_bucket]['mismatched'] == [mismatched_etag_key]
    assert boto_calls == {'GetObject': 2}  # should be 1 -- https://github.com/RaRe-Technologies/smart_open/issues/494
    assert set(o.key for o in list_objects('s3://%s/' % source_bucket)) == keys_to_delete | keys_to_keep

    # real delete
    boto_calls.clear()
    buckets = delete_files(manifest_path, dry_run=False)
    assert set(buckets[source_bucket]['deleted']) == keys_to_delete
    assert set(buckets[source_bucket]['errors']) == {deleted_etag_key}
    assert set(buckets[source_bucket]['keys']) == keys_to_delete | {deleted_etag_key}
    assert buckets[source_bucket]['mismatched'] == [mismatched_etag_key]
    assert boto_calls == {'GetObject': 2, 'DeleteObjects': 1}  # should be 1 -- https://github.com/RaRe-Technologies/smart_open/issues/494
    assert set(o.key for o in list_objects('s3://%s/' % source_bucket)) == keys_to_keep


def test_open_archived_file(s3, files, source_bucket, archive_url, manifest_path, tar_path, boto_calls):
    from s3mothball.s3mothball import open_archived_file, write_tar  # ensure mock is in place before importing functions to test

    # write tar
    strip_prefix = 'folders/'
    write_tar(archive_url, manifest_path, tar_path, strip_prefix=strip_prefix)

    for file in files:
        boto_calls.clear()
        with open_archived_file(manifest_path, tar_path, "s3://%s/%s" % (file['bucket'], file['key'])) as f:
            assert f.read() == file['contents']
        assert boto_calls == {'GetObject': 4}  # should be 2 -- https://github.com/RaRe-Technologies/smart_open/issues/494
