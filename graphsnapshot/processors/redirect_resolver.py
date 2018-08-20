"""
Extract snapshots from list of revisions.

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
import more_itertools
from typing import Iterable, Iterator, Mapping, NamedTuple

from .. import utils
from .. import file_utils as fu


DATE_START = arrow.get('2001-01-16', 'YYYY-MM')
DATE_NOW = arrow.now()


re_snapshotname = re.compile(r'snapshot\.(\d{4}-\d{2}-\d{2})\.csv\.(.+)',
                             re.IGNORECASE | re.DOTALL)


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


csv_header = ('page_id',
              'page_title',
              'revision_id',
              'revision_parent_id',
              'revision_timestamp'
              )


def read_redirects(
    redirects: pathlib.Path,
    snapshot_date: arrow.Arrow
    ) -> Mapping:

    redirects_file = fu.open_csv_file(str(redirects))
    redirects_reader = csv.reader(redirects_file)

    # skip header
    next(redirects_reader, None)

    # read redirects
    redirects_history = dict()

    redirect_line = None
    redirect_prevline = None

    is_last_revision = False

    for redirect in redirects_reader:

        if redirect is None:
                redirect_line = redirects_prevline
                is_last_revision = True
        else:
            # 0: page_id
            # 1: page_title
            # 2: revision_id
            # 3: revision_parent_id
            # 4: revision_timestamp,
            # 5: revision_minor
            # 6: redirect.target
            # 7: redirect.tosection
            redirect_line = {
                'page_id': int(redirect[0]),
                'page_title': redirect[1],
                'revision_id': int(redirect[2]),
                'revision_parent_id': int(redirect[3]) \
                                        if redirect[3] else -1,
                'revision_timestamp': arrow.get(redirect[4]),
                'revision_minor': int(redirect[5]),
                'redirect.target': redirect[6],
                'redirect.tosection': redirect[7]
                }


        if is_last_revision or \
                redirect_line['revision_timestamp'] > snapshot_date:
            red_dict = redirect_prevline

            try:
                red = Redirect(Page(red_dict['page_id'],
                                   red_dict['page_title'],
                                   Revision(red_dict['revision_id'],
                                            red_dict['revision_parent_id'],
                                            red_dict['revision_timestamp'],
                                            red_dict['revision_minor']
                                            )
                                   ),
                              red_dict['redirect.target'],
                              red_dict['redirect.tosection']
                              )
            except TypeError as err:
                continue

            redirects_history[red_dict['page_title']] = red

        else:
            redirect_prevline = redirect_line

    return redirects_history


def process_lines(
        dump: Iterable[list],
        timestamps: Iterable[arrow.arrow.Arrow],
        stats: Mapping,
        only_last_revision: bool) -> Iterator[list]:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    # skip header
    # header = next(dump)
    next(dump)
    header = csv_header

    # FIXME: implement this function, for now return an empty generator
    return (_ for _ in ())


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'resolve-redirect',
        help='Resolve redirects in a snapshot.',
    )
    parser.add_argument(
        '--redirects',
        type=pathlib.Path,
        required=True,
        help='File with redirects over the snapshot history.'
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

    redirects = args.redirects

    match = re_snapshotname.match(basename)
    if match:
        snapshot_date = arrow.get(match.group(1), 'YYYY-MM-DD')
    assert (snapshot_date > DATE_START and snapshot_date < DATE_NOW)
    # snapshot_date = snapshot_date.strftime('%Y-%m-%d')

    redirects_history = read_redirects(redirects, snapshot_date)

    writers = {}
    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/(basename + '.stats.xml')),
            compression=args.output_compression,
        )

        writer = csv.writer(pages_output)

    pages_generator = process_lines(
        dump,
        redirects_history=redirects_history
    )

    writer.writerow(csv_header)
    for page in pages_generator:
        writer.writerow((
            page.id,
            page.title,
            page.revision.id,
            page.revision.parent_id,
            page.revision.timestamp,
        ))
