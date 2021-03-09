import codecs
import csv
import gzip
import io
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from typing import IO, List

import requests
from dateutil.parser import parse
from requests.models import Response

## This is a copy of https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/tools/epa/-/blob/master/src/python/internal/io_utils.py
## TODO Create a wheel

class DownloadTask:
    def __init__(self, destination: str, urls: List = None, metadata = None):
        self.destination = destination
        if urls:
            self.urls = urls
        else:
            self.urls = []
        self.metadata = metadata

    def add_url(self, url: str):
        self.urls.append(url)

    def reset(self):
        os.remove(self.destination)

    def __str__(self):
        dest = os.path.abspath(self.destination)
        if len(self.urls) == 1:
            return "{} ==> {}".format(self.urls[0], dest)
        return "[{:d}]==> {}".format(len(self.urls), dest)

    def is_up_to_date(self, is_transformed: bool = True):
        if len(self.urls) == 1 and not is_transformed:
            return is_downloaded(self.urls[0], self.destination)
        for url in self.urls:
            if not is_downloaded(url, self.destination, 1000):
                return False
        return True


def as_stream(url: str, extension: str = ".csv"):
    """
    Returns teh content of URL as a stream. In case the content is in zip
    format (excluding gzip) creates a temporary file

    :param url: URL
    :param extension: optional, when the content is zip-encoded, the extension
        of the zip entry to return
    :return: Content of the URL or a zip entry
    """
    response = requests.get(url, stream=True)
    check_http_response(response)
    raw = response.raw
    if url.lower().endswith(".zip"):
        tfile = tempfile.TemporaryFile()
        download(url, tfile)
        tfile.seek(0)
        zfile = zipfile.ZipFile(tfile)
        entries = [
            e for e in zfile.namelist() if e.endswith(extension)
        ]
        assert len(entries) == 1
        stream = io.TextIOWrapper(zfile.open(entries[0]))
    else:
        stream = raw
    return stream


def as_csv_reader(url: str):
    """
    An utility method to return the CSV content of the URL as CSVReader

    :param url: URL
    :return: an instance of csv.DictReader
    """
    stream = as_stream(url)
    reader = csv.DictReader(stream, quotechar='"', delimiter=',',
        quoting=csv.QUOTE_NONNUMERIC, skipinitialspace=True)
    return reader


def fopen(path: str, mode: str):
    """
    A wrapper to open various types of files

    :param path: Path to file
    :param mode: Opening mode
    :return: file-like object
    """
    if isinstance(path, io.BufferedReader):
        return codecs.getreader("utf-8")(path)
    if path.lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, mode))
    if 'b' in mode:
        return open(path, mode)
    return open(path, mode, encoding="utf-8")


def check_http_response(r: Response):
    """
    An internal method raises an exception of HTTP response is not OK

    :param r: Response
    :return: nothing, raises an exception if response is not OK
    """
    if not r.ok:
        msg = "HTTP Response: {:d}; Reason: {}".format(r.status_code, r.reason)
        raise Exception(msg)


def download(url: str, to: IO):
    """An utility method to download large binary data to a file-like object"""
    response = requests.get(url, stream=True)
    for chunk in response.iter_content(chunk_size=1048576):
        to.write(chunk)
        print('#', end='')
    print('.', end=' ')
    return


def is_downloaded(url: str, target: str, check_size: int = 0) -> bool:
    """
    Checks if the same data has already been downloaded

    :param check_size: Use default value (0) if target size should be equal
        to source size. If several urls are combined when downloaded
        then specify a positive integer to check that destination file
        size is greater than the specified value. Specifying negative
        value will disable size check
    :param url: URL with data
    :param target: Destination of teh downloads
    :return: True if the destination file exists and is newer than
        URL content
    """
    if os.path.isfile(target):
        response = requests.head(url, allow_redirects=True)
        check_http_response(response)
        headers = response.headers
        remote_size = int(headers.get('content-length', 0))
        remote_date = parse(headers.get('Last-Modified', 0))
        stat = os.stat(target)
        local_size = stat.st_size
        local_date = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        date_check = local_date > remote_date
        if check_size == 0:
            size_check = local_size == remote_size
        else:
            size_check = local_size > check_size
        return date_check and size_check
    return False


def write_csv(reader: csv.DictReader,
              writer: csv.DictWriter,
              transformer=None,
              filter=None,
              write_header: bool = True):
    """
    Rewrites the CSV content optionally transforming and
    filtering rows

    :param transformer: An optional callable that tranmsforms a row in place
    :param reader: Input data as an instance of csv.DictReader
    :param writer: Output source should be provided as csv.DictWriter
    :param filter: Optionally, a callable function returning True
        for rows that should be written to the output and False for those
        that should be omitted
    :param write_header: whether to first write header row
    :return: Nothing
    """
    counter = 0
    if write_header:
        writer.writeheader()
    for row in reader:
        if transformer:
            transformer(row)
        if (not filter) or filter(row):
            writer.writerow(row)
        counter += 1
        if (counter % 10000) == 0:
            print("*", end="")
    print()


