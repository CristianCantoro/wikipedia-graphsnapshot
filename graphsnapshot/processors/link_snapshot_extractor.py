"""
Extract links from list of revisions.

The output format is csv.
"""

import sys
import csv
import collections
import datetime
import functools

import jsonable
import more_itertools
import mwxml
import arrow
from typing import Iterable, Iterator, Mapping, NamedTuple, Optional

from .. import utils
from .. import file_utils as fu


Wikilink = NamedTuple('Wikilink', [
    ('link', str),
    ('anchor', int),
    ('section_name', str),
    ('section_level', int),
    ('section_number', int),
])


Revision = NamedTuple('Revision', [
    ('id', int),
    ('parent_id', int),
    ('user_type', str),
    ('username', str),
    ('minor', bool),
    ('timestamp', arrow.arrow.Arrow),
    ('wikilink', Wikilink)
])


Page = NamedTuple('Page', [
    ('id', int),
    ('title', str),
    ('revision', Revision),
])


csv_header = ('page_id',
              'page_title',
              'revision_id',
              'revision_parent_id',
              'revision_timestamp',
              'user_type',
              'user_username',
              'user_id',
              'revision_minor',
              'wikilink.link',
              'wikilink.anchor',
              'wikilink.section_name',
              'wikilink.section_level',
              'wikilink.section_number',
              )

snapshot_csv_header = ('page_id',
                       'page_tile',
                       'revision_id',
                       'revision_parent_id'
                       'timestamp'
                       )


def process_lines(
        dump: Iterable[list],
        date: arrow.arrow.Arrow,
        pages_in_snapshot: set,
        revisions_in_snapshot: set,
        stats: Mapping) -> Iterator[list]:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    # skip header
    # header = next(dump)
    next(dump)
    header = csv_header

    dump_page = None
    dump_prevpage = None

    i = 0
    page_revisions = []

    old_revision = None
    revision = None
    is_last_revision = False

    skip_page = False

    # equivalent to
    # for revision in dump:
    while True:
        old_revision = revision
        try:
            revision = next(dump, None)
        except:
            import pdb
            pdb.set_trace()

        # if revision is None:
        #         revision = old_revision
        #         is_last_revision = True

        # revision_data = dict(zip(header, revision))

        # dump_page = Page(
        #     int(revision_data['page_id']),
        #     revision_data['page_title'],
        #     Revision(int(revision_data['revision_id']),
        #              int(revision_data['revision_parent_id'])
        #              if revision_data['revision_parent_id'] else None,
        #              revision_data['user_type'],
        #              revision_data['user_username'],
        #              revision_data['revision_minor'],
        #              arrow.get(revision_data['revision_timestamp']),
        #              Wikilink(revision_data['wikilink.link'],
        #                       revision_data['wikilink.anchor'],
        #                       revision_data['wikilink.section_name'],
        #                       int(revision_data['wikilink.section_level']),
        #                       int(revision_data['wikilink.section_number']),
        #                       )))

        # # print("skip_page: {}".format(skip_page))

        # if dump_prevpage and skip_page and dump_prevpage.id == dump_page.id:
        #     dump_prevpage = dump_page
        #     continue
        # else:
        #     skip_page = False

        # if dump_prevpage is None or dump_prevpage.id != dump_page.id:
        #     utils.log("Processing ", dump_page.title)
        # else:
        #     if dump_prevpage.revision.id != dump_page.revision.id:
        #         utils.dot()

        # if dump_page.id not in pages_in_snapshot:
        #     skip_page = True
        #     dump_prevpage = dump_page

        #     print(" -> skip", end='', file=sys.stderr, flush=True)
        #     continue

        # if (is_last_revision and
        #         (dump_page.page.id in pages_in_snapshot or
        #          dump_page.revision.id in revisions_in_snapshot)):
        #     import pdb
        #     pdb.set_trace()

        #     sorted_revisions = sorted(page_revisions,
        #                               key=lambda pg: pg.revision.timestamp)

        #     dump_prevpage = dump_page
        #     page_revisions = [dump_page]

        #     i = 0
        #     j = 0
        #     prevpage = None
        #     break_flag = False
        #     while j < len(sorted_revisions):
        #         page = sorted_revisions[j]

        #         ct = page.revision.timestamp
        #         pt = prevpage.revision.timestamp if prevpage else EPOCH

        #         while i < len(timestamps):
        #             ts = timestamps[i]
        #             yield (page, ts)

        #         if is_last_revision:
        #             break
        # else:
        #     dump_prevpage = dump_page


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'link-snapshot-extractor',
        help='Extract link snapshots from page list',
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Reference date'
    )
    parser.add_argument(
        '--snapshot-file',
        type=str,
        help='Snapshot file.'
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
        },
        'section_names': {
            'global': collections.Counter(),
            'last_revision': collections.Counter(),
        },
    }

    date = arrow.get(args.date)

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       (basename + '.features.{date}.csv'))
        filename = filename.format(date=date.format('YYYY-MM-DD'))

        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/(basename + '.stats.xml')),
            compression=args.output_compression,
        )

        writer = csv.writer(pages_output)

    snapshot_infile = fu.open_csv_file(args.snapshot_file)
    snapshot_reader = csv.reader(fu.open_csv_file(snapshot_infile))

    pages_in_snapshot = set()
    revisions_in_snapshot = set()
    for row_data in snapshot_reader:
        pages_in_snapshot.add(int(row_data[0]))
        revisions_in_snapshot.add(int(row_data[2]))

    pages_generator = process_lines(
        dump,
        date=date,
        pages_in_snapshot=pages_in_snapshot,
        revisions_in_snapshot=revisions_in_snapshot,
        stats=stats,
    )

    for page in pages_generator:
        writer.writerow((
            page.id,
            page.title,
            page.revision.id,
            page.revision.parent_id,
            page.revision.timestamp,
        ))
