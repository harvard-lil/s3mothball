import concurrent.futures
import copy
import hashlib
import tarfile
from io import BytesIO
from pathlib import Path


class HashingFile:
    """ File wrapper that stores a hash of the read or written data. """
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
    def addfile(self, tarinfo, fileobj=None):
        """ Set tarinfo.offset and tarinfo.offset_data. """
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


def threaded_queue(func, items, max_workers):
    """
        Create a thread pool to call func with each argument list in items, yielding each result as it is ready.
        Implements backpressure; will not work on more than max_workers items at a time.
        Return order is not guaranteed.
    """
    items = iter(items)
    futures = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        def queue_item():
            try:
                item = next(items)
            except StopIteration:
                return
            futures.add(executor.submit(func, *item))
        for i in range(max_workers):
            queue_item()
        while futures:
            future = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)[0].pop()
            yield future.result()
            futures.remove(future)
            queue_item()