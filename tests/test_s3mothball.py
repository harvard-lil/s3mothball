import csv
import tarfile

import pytest
from smart_open import open


def test_write_tar(s3, files, source_bucket, archive_url, manifest_path, tar_path):
    from s3mothball.s3mothball import write_tar  # ensure mock is in place before importing functions to test

    # run write_tar
    strip_prefix = 'folders/'
    write_tar(archive_url, manifest_path, tar_path, strip_prefix=strip_prefix)

    # check tar file
    files_by_key = {e['key'][len(strip_prefix):]: e for e in files}
    with open(tar_path, 'rb', ignore_ext=True) as f:
        tar = tarfile.TarFile.open(fileobj=f, mode='r|')
        for member in tar:
            assert member.name in files_by_key
            entry = files_by_key[member.name]
            assert tar.extractfile(member).read() == entry['contents'].encode('utf8')
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
            }
            for entry in files
        ]
        entries = list(csv.DictReader(f))
        assert entries == expected_entries


def test_validate_tar(s3, files, source_bucket, archive_url, manifest_path, tar_path):
    from s3mothball.s3mothball import write_tar, validate_tar  # ensure mock is in place before importing functions to test

    # run write_tar
    strip_prefix = 'folders/'
    write_tar(archive_url, manifest_path, tar_path, strip_prefix=strip_prefix)

    # validates successfully
    validate_tar(manifest_path, tar_path, strip_prefix=strip_prefix)

    # load manifest
    with open(manifest_path, newline='') as f:
        manifest = list(csv.DictReader(f))
    with open(tar_path, 'rb', ignore_ext=True) as f:
        tar_contents = f.read()
    def write_manifest(rows):
        with open(manifest_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    # manifest missing a line
    write_manifest(manifest[:-1])
    with pytest.raises(ValueError, match=r"Not enough files found in manifest"):
        validate_tar(manifest_path, tar_path, strip_prefix=strip_prefix)

    # manifest has extra line
    write_manifest(manifest+[manifest[-1]])
    with pytest.raises(ValueError, match=r"Manifest files not found in tar"):
        validate_tar(manifest_path, tar_path, strip_prefix=strip_prefix)

    # manifest with wrong hashes
    write_manifest([{**m, 'TarMD5': 'foo'} for m in manifest])
    with pytest.raises(ValueError, match=r"File hash mismatch"):
        validate_tar(manifest_path, tar_path, strip_prefix=strip_prefix)

    # manifest with wrong names
    write_manifest([{**m, 'Key': m['Key']+'abc'} for m in manifest])
    with pytest.raises(ValueError, match=r"Mismatched keys"):
        validate_tar(manifest_path, tar_path, strip_prefix=strip_prefix)

    # detect tar corrupted
    with open(tar_path, 'wb', ignore_ext=True) as f:
        f.write(b'ABCD'+tar_contents)
    with pytest.raises(tarfile.ReadError):
        validate_tar(manifest_path, tar_path, strip_prefix=strip_prefix)
