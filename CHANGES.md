# Changes in 0.2.1-RC

-   Make sure to only work with regular files

    This solves problems that arised when there were symlinks in the search
    path, or when a file was scanned in a previous run and afterwards there
    is a directory with the same path and name. This could have resulted in
    "unreachable code" errors or random panics.

-   Performance boost due to skipping needless object creations

# Changes in 0.2.0

-   Open with write permission to set modtime

    Some operating systems like Windows need write access to a file to be
    able to set the modified time.

    By fixing this issue, we are able to run all tests also on Windows and
    enable Windows builds for new releases.

-   Make declutter level configurable

-   Handle no cache file correctly

-   Read files more efficiently

-   Write files more efficiently

-   Open each file just once and read in parallel

-   Introduce cache format v1

    Besides many internal fixes and optimizations, the new cache format brings:

    -   Short keys: No more "nanos_since_epoch" all over the place, instead we use one-letter-keys now.
    -   Hashing algorithm is only stored once now.
    -   Paths are stored in a nested fashion.

    This results in a much smaller cache file than before, even with bigger dedupe caches.

    Furthermore, the cache format version is implemented in a way that it will always be backward compatible. Older
    cache formats will always be valid for reading. For writing, the latest cache format will be used.

-   Add delegate method to OnceCell::take

-   Add separate method to list missing chunks

    That way, the check_cache method can access this list and fail fast and
    silently.

# Changes in 0.1.0

Initial release.
