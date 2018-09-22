"""
Resolve redirects in a snapshot.

The output format is csv.
"""

import os
import csv
import mwxml
import arrow
import regex as re
import pathlib
import jsonable
import datetime
import functools
import collections
from typing import Iterable, Iterator, Mapping, NamedTuple

import more_itertools

from .. import utils
from .. import file_utils as fu
from .. import dumper


NLINES = 10000

DATE_START = arrow.get('2001-01-16', 'YYYY-MM')
DATE_NOW = arrow.now()


# example filenames:
#   * enwiki-20150901-pages-meta-history1.xml-p000000010p000002861.7z.features.xml.gz
chunk_regex =  r'([a-z]{2})wiki-(\d{8})'
chunk_regex += r'-pages-meta-history(\d{1,2})\.xml'
chunk_regex += r'-p(\d+)p(\d+)\.(gz|bz2|7z)'
chunk_regex += r'\.features\.xml(\.[^\.]+)(\.(gz|bz2|7z))?'
re_chunk = re.compile(chunk_regex, re.IGNORECASE | re.DOTALL)

# new chunk glob
#   * enwiki-20180301-pages-meta-history1.xml-p10p2115.7z.features.xml.gz
#     {lang}wiki-{date}-pages-meta-history{historyno}.xml
#       -p{pageid_first}p{pageid_last}.{dumpext}.features.xml.{ext}
new_chunk_glob = '{lang}wiki-{date}'
new_chunk_glob += '-pages-meta-history{historyno}.xml'
new_chunk_glob += '-p{pageid_first}p{pageid_last}.{dumpext}'
new_chunk_glob += '.features.xml.{ext}'


Revision = NamedTuple('Revision', [
    ('id', int),
    ('parent_id', int),
    ('timestamp', jsonable.Type),
    ('minor', bool),
])


Page = NamedTuple('Page', [
    ('id', str),
    ('title', str),
    ('revision', Revision),
])

# - Redirect:
#   - Page:
#     - page_id
#     - page_title
#     - Revision:
#       - revision_id
#       - revision_parent_id
#       - revision_timestamp,
#       - revision_minor
#   - target
#   - tosection
Redirect = NamedTuple('Redirect', [
    ('page', Page),
    ('target', str),
    ('tosection', str),
])


csv_header_input = ('page_id',
                    'page_title',
                    'revision_id',
                    'revision_parent_id',
                    'revision_timestamp'
                    )


csv_header_output = csv_header_input + ('redirect_id',
                                        'redirect_title',
                                        'redirect_revision_id',
                                        'redirect_revision_parent_id',
                                        'redirect_revision_timestamp'
                                        )


stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <redirects_analyzed>${stats['performance']['redirects_analyzed'] | x}</redirects_analyzed>
        <pages_analyzed>${stats['performance']['pages_analyzed'] | x}</pages_analyzed>
    </performance>
</stats>
'''


def read_snapshot_pages(
    input_file_path: Iterable[list]) -> Mapping:

    snapshot = fu.open_csv_file(str(input_file_path))
    snapshot_reader = csv.reader(snapshot)

    title2id = dict()

    counter = 0
    print('\nReading snapshot ', end=' ')
    for line in snapshot_reader:
        if counter % NLINES == 0:
            utils.dot()
        counter = counter + 1

        page_title = line[1]
        page_id = int(line[0])

        title2id[page_title] = page_id

    return title2id


def normalize_title(title: str) -> str:
    if len(title) > 1:
        title = title[0].upper() + title[1:]
    elif len(title) == 1:
        title = title[0].upper()

    return title.replace('_', ' ')


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        snapshot_title2id: Mapping,
        redirects_history: Mapping) -> Iterator[list]:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    # skip header
    # header = next(dump)
    next(dump)
    header = csv_header_input

    counter = 0
    print('\nProcess snapshot ', end=' ')
    for snapshot_page in dump:
        if counter % NLINES == 0:
            utils.dot()

        counter = counter + 1
        # get only page id and page title
        resolved = resolve_redirect(page=snapshot_page,
                                    original_page=snapshot_page,
                                    stats=stats,
                                    snapshot_title2id=snapshot_title2id,
                                    redirects_history=redirects_history,
                                    count_recursive_calls=0,
                                    )

        stats['performance']['pages_analyzed'] += 1

        yield snapshot_page + resolved


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'compare-extractions',
        help='Compare extractions.',
    )
    parser.add_argument(
        '--new-extractions-dir',
        type=pathlib.Path,
        help='Directory with the new extractions.'
    )

    parser.add_argument(
        '--old-extraction-date',
        default=None,
        help='Last date from new extraction [default: infer from filename].'
    )

    parser.add_argument(
        '--new-extraction-date',
        action='store_true',
        default=None,
        help='Last date from new extraction [default: infer from filename].'
    )

    parser.add_argument(
        '--ignore-newer',
        action='store_true',
        help='Ignore additions in the new extractions that are newer '
             'than the last date in the old extraction.'
    )

    parser.set_defaults(func=main)


def main(
        dump: Iterable[list],
        basename: str,
        args) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'revisions_analyzed': 0,
            'pages_analyzed': 0,
        }
    }
    stats['performance']['start_time'] = datetime.datetime.utcnow()


    inputfile_full_path = [afile for afile in args.files
                           if afile.name == basename][0]

    new_extractions_dir = args.new_extractions_dir
    ignore_newer = args.ignore_newer

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       (basename + '.compare_extractions.features.csv'))
        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/
                    (basename + '.compare_extractions.stats.xml')),
            compression=args.output_compression,
        )
    writer = csv.writer(pages_output)

    base_match = re_chunk.match(basename)
    if base_match:
        base_chunk_lang = base_match.group(1)
        base_chunk_date = base_match.group(2)
        base_chunk_historyno = base_match.group(3)
        base_chunk_pageid_first = int(base_match.group(4).lstrip('0'))
        base_chunk_pageid_last = int(base_match.group(5).lstrip('0'))
        base_chunk_dumpext = base_match.group(6)

        base_chunk_ext = ''
        try:
            # ignore groups 7 and 8
            base_chunk_ext = base_match.group(9)
        except IndexError:
            pass

    if args.old_extraction_date is None:
        old_extraction_date = arrow.get(base_chunk_date, 'YYYYMMDD')
    else:
        old_extraction_date = arrow.get(args.old_extraction_date)

    assert (old_extraction_date > DATE_START and \
                old_extraction_date < DATE_NOW)

    import ipdb; ipdb.set_trace()

    if args.old_extraction_date is None:
        new_extraction_date = arrow.get(base_chunk_date, 'YYYYMMDD')
    else:
        new_extraction_date = arrow.get(args.new_extraction_date)
    assert (new_extraction_date > DATE_START and \
                new_extraction_date < DATE_NOW)

   
    dump = csv.reader(dump)
    pages_generator = process_lines(
        dump,
        stats,
        new_extractions_dir=new_extractions_dir,
        ignore_newer=ignore_newer
        )

    writer.writerow(csv_header_output)
    for page in pages_generator:
        # csv_header_output
        #
        # page_id
        # page_title
        # revision_id
        # revision_parent_id
        # revision_timestamp
        # redirect_id
        # redirect_title
        # redirect_revision_id
        # redirect_revision_parent_id
        # redirect_revision_timestamp
        writer.writerow(page)

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
