# how much of a source file should we store in ram before spooling to disk?
# ram usage will include this number of bytes * THREADS
SPOOLED_FILE_SIZE = 10 * 2 ** 20

# how many worker threads to fetch files in the background for archiving?
# just has to be enough to load items from S3 faster than a single thread can tar them.
# 8 seems to be enough
THREADS = 8