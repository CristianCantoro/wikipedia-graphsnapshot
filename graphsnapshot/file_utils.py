"""Main module that parses command line arguments."""
import codecs
import os
import subprocess
import gzip
import io
import csv

import pathlib
from typing import IO, Optional, Union

import subprocess

import compressed_stream as cs


def open_csv_file(path: Union[str, IO]):
    """Open a csv file, decompressing it if necessary."""
    f = cs.functions.open_file(
        cs.functions.file(path)
    )
    return f


def compressor_7z(file_path: str):
    """"Return a file-object that compresses data written using 7z."""
    p = subprocess.Popen(
        ['7z', 'a', '-si', file_path],
        stdin=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    return io.TextIOWrapper(p.stdin, encoding='utf-8')


def output_writer(path: str, compression: Optional[str]):
    """Write data to a compressed file."""
    if compression == '7z':
        return compressor_7z(path + '.7z')
    elif compression == 'gzip':
        return gzip.open(path + '.gz', 'wt', encoding='utf-8')
    else:
        return open(path, 'wt', encoding='utf-8')


def create_path(path: Union[pathlib.Path, str]):
    """Create a path, which may or may not exist."""
    path = pathlib.Path(path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True)
