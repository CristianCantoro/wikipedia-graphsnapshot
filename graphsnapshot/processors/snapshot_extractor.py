"""
Extract snapshots from list of revisions.

The output format is csv.
"""

import csv
import collections
import datetime
import functools

import fuzzywuzzy.process
import jsonable
import more_itertools
import mwxml
import arrow
from typing import Iterable, Iterator, Mapping, NamedTuple


WIKIEPOCH = arrow.get(datetime.datetime(2001, 1, 15))
EPOCH = arrow.get(datetime.datetime.fromtimestamp(0))
NOW = arrow.utcnow()
DELTA = NOW - WIKIEPOCH

PERIODICITY = {
    'd': lambda n: {'days': n},
    'w': lambda n: {'weeks': n},
    'M': lambda n: {'months': n},
    'y': lambda n: {'years': n},
}


NPERIODS = {
    'd': lambda days: days + 2,
    'w': lambda days: int(days/7) + 2,
    'M': lambda days: int(days/28) + 2,
    'y': lambda days: int(days/365) + 2
}

Revision = NamedTuple('Revision', [
    ('id', int),
    ('parent_id', int),
    ('timestamp', jsonable.Type),
])


Page = NamedTuple('Page', [
    ('id', str),
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
              'revision_minor'
              )


def process_lines(
        dump: Iterable[list],
        timestamps: Iterable[arrow.arrow.Arrow],
        stats: Mapping,
        only_last_revision: bool) -> Iterator[list]:
    """Extract foo from bar."""

    # skip header
    # header = next(dump)
    next(dump)
    header = csv_header

    prevpage = None
    for revision in dump:

        revision = dict(zip(header, revision))
        # Let:
        # prevpage  be the id of the page that we analyzed in the previous
        #           revision.
        # page      be the id of the page we are processing now in the
        #           dump.
        # ct        be the timestamp of the revision we are processing now
        #           in the dump.
        # pt        be the timestamp of the previous revision.
        # prevts    be the previous timestamp that we tried
        # ts        be the timestamp of the snapshot that we want to
        #           create.
        #
        # CASE 1 - prevpage and page are the same
        #
        # We have that:
        # * if prevage is None, then pt = EPOCH (-inf)
        # * pt <= ct for all revisions (implied).
        #
        # then:
        # if pt > ts:
        #     # ct > ts is implied, so ct >= pt > ts
        #     # the timestamps of all the revisions that we want to
        #     # analyze will be greater than ts
        #     jump to new page
        #
        # elif ct > ts:
        #     # pt <= ts is implied, so pt <= ts < cs
        #     the previous revision is in the snapshot
        #
        # else:
        #     # pt <= ts and ct <= ts, so pt <= ct <= ts
        #     # there may be a further time in the snapshopt
        #     check another revision
        #
        # CASE 2 - prevpage and page differ
        #
        # If prevpage and pagetitle differ then pt was the maximum
        # revision available.
        #
        # prevpage is in the snapshot with pt, we still have to
        # check ct for the current page. In this case the pt
        # wrt the current page is the EPOCH and prevpage wrt
        # the current page is None.

        # prevpage is set outside the loop to None
        # page = get value

        # ct = get value
        # pt = get_value if prevpage is not None or EPOCH

        page = Page(revision['page_id'],
                    revision['page_title'],
                    Revision(revision['revision_id'],
                             revision['revision_parent_id'],
                             arrow.get(revision['revision_timestamp'])
                             )
                    )

        utils.log("Processing", page.title)
        for ts in timestamps:

            import pdb
            pdb.set_trace()

            if prevpage and prevpage != pagetitle:
                # the previous revision is in the snapshot

                if ct > ts:
                    # the page did not exist at the time
                    # check another timestamp
                    continue

                else:
                    # ct <= ts
                    # check another revision
                    break

            else:
                # prevpage is None or prevpage == pagetitle
                if pt > ts:
                    # jump to a new page. but before check the other
                    # timestamps
                    continue

                elif ct > ts:
                    # the previous revision is in the snapshot
                    # check another timestamp
                    yield (page, ts)
                    continue

                else:
                    # check another revision
                    break

            # update step
            # pt = ct
            # prevpage = page


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'snapshot-extractor',
        help='Extract snapshot from page list',
    )
    parser.add_argument(
        '--periodicity',
        type=str,
        choices=['d', 'w', 'M', 'y'],
        default='M',
        help='Produce snapshot with daily (d), weekly (w), monthly (M) or'
             'yearly periodicity (default = "M").'
    )
    parser.add_argument(
        '--only-last-revision',
        action='store_true',
        help='Consider only the last revision for each page.',
    )

    parser.set_defaults(func=main)


def main(
        dump: Iterable[list],
        features_output_h,
        stats_output_h,
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

    period = PERIODICITY[args.periodicity]
    nperiods = NPERIODS[args.periodicity](DELTA.days)
    timestamps = [WIKIEPOCH.replace(**period(i))
                  for i in range(nperiods)
                  if WIKIEPOCH.replace(**period(i)) <= NOW.replace(**period(1))
                  ]

    writers = {}
    for ts in timestamps:
        filename = features_output_h.name.replace(".csv", '.{date}.csv'.format(
            date=ts.format('YYYY-MM-DD')))
        writer = csv.writer(features_output_h)
        writers[ts] = writer

    pages_generator = process_lines(
        dump,
        timestamps=timestamps,
        stats=stats,
        only_last_revision=args.only_last_revision,
    )

    for page, ts in pages_generator:
        writers[ts].writerow((
            page.id,
            page.title,
            page.revision.id,
            page.revision.parent_id,
            page.revision.timestamp,
        ))
