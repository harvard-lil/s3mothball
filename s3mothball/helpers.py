import concurrent.futures
import copy
import csv
import hashlib
import itertools
import tarfile
from io import BytesIO
from pathlib import Path
from shutil import copyfileobj
from tempfile import SpooledTemporaryFile

import boto3
from smart_open import open
from smart_open.s3 import parse_uri

from s3mothball.settings import SPOOLED_FILE_SIZE, THREADS


class HashingFile:
    """ File wrapper that stores a hash and size of the read or written data. """
    def __init__(self, source, hash_name='md5'):
        self._sig = hashlib.new(hash_name)
        self._source = source
        self.length = 0

    def read(self, *args, **kwargs):
        result = self._source.read(*args, **kwargs)
        self.update_hash(result)
        return result

    def write(self, value, *args, **kwargs):
        self.update_hash(value)
        return self._source.write(value, *args, **kwargs)

    def update_hash(self, value):
        self._sig.update(value)
        self.length += len(value)

    def hexdigest(self):
        return self._sig.hexdigest()

    def __getattr__(self, attr):
        return getattr(self._source, attr)


class LoggingTarFile(tarfile.TarFile):
    """ TarFile subclass that sets tarinfo.offset and tarinfo.offset_data on records when written. """
    def addfile(self, tarinfo, fileobj=None):
        tarinfo = copy.copy(tarinfo)
        buf = tarinfo.tobuf(self.format, self.encoding, self.errors)
        tarinfo.offset = self.offset
        tarinfo.offset_data = self.offset + len(buf)
        super().addfile(tarinfo, fileobj)


class TeeFile:
    """
        File wrapper that tees a file object so it can be read multiple times.

        >>> source = BytesIO(b'12345678')
        >>> f1, f2 = TeeFile.tee(source)
        >>> assert f1.read(2) == f2.read(2) == b'12'
        >>> assert f1.read(2) == f2.read(2) == b'34'
        >>> assert f2.read(2) == f1.read(2) == b'56'
        >>> assert f2.read(2) == f1.read(2) == b'78'
        >>> assert f1.my_buffer is f2.your_buffer == []
        >>> assert f2.my_buffer is f1.your_buffer == []
    """
    def __init__(self, source, my_buffer, your_buffer):
        self.source = source
        self.my_buffer = my_buffer
        self.your_buffer = your_buffer
        self.pos = 0

    def read(self, size):
        if size == 0:
            return b''
        out = b''
        if self.my_buffer:
            s = b''.join(self.my_buffer)
            out, s = s[:size], s[size:]
            self.my_buffer.clear()
            if s:
                self.my_buffer.append(s)
            size -= len(out)
        if size:
            extra = self.source.read(size)
            self.your_buffer.append(extra)
            out += extra
        self.pos += len(out)
        return out

    def tell(self):
        return self.pos

    @classmethod
    def tee(cls, f):
        b1 = []
        b2 = []
        return cls(f, b1, b2), cls(f, b2, b1)


class OffsetSizeFile:
    """
        File wrapper that reveals a subsection of a larger file.

        >>> source = BytesIO(b'12345678')
        >>> wrapped = OffsetSizeFile(source, 2, 4)
        >>> assert wrapped.read(2) == b'34'
        >>> assert wrapped.read() == b'56'
        >>> assert wrapped.read() == wrapped.read(2) == b''
    """
    def __init__(self, source, offset, size):
        self.source = source
        self.offset = offset
        self.size = size
        source.seek(offset)
        self.pos = 0

    def read(self, size=None):
        if size is None:
            size = self.size - self.pos
        else:
            size = min(size, self.size - self.pos)
        out = self.source.read(size)
        self.pos += len(out)
        return out


def make_parent_dir(path):
    if path.startswith('s3://'):
        return
    Path(path).parent.mkdir(exist_ok=True, parents=True)


def threaded_queue(func, items):
    """
        Create a thread pool to call func with each argument list in items, yielding each result as it is ready.
        Implements backpressure: will not work on more than THREADS items at a time.
        Return order is not guaranteed.
    """
    items = iter(items)
    futures = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        def queue_item():
            try:
                item = next(items)
            except StopIteration:
                return
            futures.add(executor.submit(func, *item))
        for i in range(THREADS):
            queue_item()
        while futures:
            future = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)[0].pop()
            yield future.result()
            futures.remove(future)
            queue_item()


def write_dicts_to_csv(manifest_path, rows):
    with open(manifest_path, 'w', newline='') as out:
        writer = csv.DictWriter(out, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def read_dicts_from_csv(manifest_path):
    with open(manifest_path, newline='') as f:
        for row in csv.DictReader(f):
            yield row


def list_objects(s3_url):
    source_path_parsed = parse_uri(s3_url)
    bucket = boto3.resource('s3').Bucket(source_path_parsed['bucket_id'])
    key = source_path_parsed['key_id'].rstrip('/')
    if key:
        key += '/'
    return bucket.objects.filter(Prefix=key)


def load_object(obj, temp_dir):
    """
        Load S3 object `obj` into SpooledTemporaryFile `body` stored in `temp_dir`.
        Return (obj, response, body).
    """
    response = obj.get()
    body = SpooledTemporaryFile(SPOOLED_FILE_SIZE, dir=temp_dir)
    copyfileobj(response['Body'], body)
    body.seek(0)
    return obj, response, body


def chunks(iterable, size=1000):
    """
        Iterate over iterable in chunks of size `size`.

        >>> assert list(chunks([1,2,3,4,5], 2)) == [(1,2), (3,4), (5,)]
    """
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk