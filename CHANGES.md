# Changes since latest release

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

# Changes in 0.1.0

Initial release.
