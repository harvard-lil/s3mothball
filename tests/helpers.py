from datetime import timezone

from moto.core.utils import str_to_rfc_1123_datetime


def write_file(s3, bucket, key, contents):
    response = s3.put_object(Bucket=bucket, Key=key, Body=contents)
    headers = response['ResponseMetadata']['HTTPHeaders']
    return {
        'bucket': bucket,
        'key': key,
        'contents': contents.encode('utf8'),
        'etag': headers['etag'].strip('"'),
        'modified': str_to_rfc_1123_datetime(headers['last-modified']).replace(tzinfo=timezone.utc),
        'size': headers['content-length'],
    }