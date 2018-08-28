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


MAX_RECURSION = 10
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


def read_redirects(
    redirects: pathlib.Path,
    snapshot_date: arrow.Arrow) -> Mapping:

    redirects_file = fu.open_csv_file(str(redirects))
    redirects_reader = csv.reader(redirects_file)

    # skip header
    next(redirects_reader, None)

    # read redirects
    redirects_history = dict()

    redirect_line = None
    redirect_prevline = None

    is_last_revision = False

    counter = 0
    print('\nReading redirects ', end=' ')
    for redirect in redirects_reader:
        if counter % NLINES == 0:
            utils.dot()
        counter = counter + 1

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

            redirects_history[red_dict['page_id']] = red

        else:
            redirect_prevline = redirect_line

    return redirects_history


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


def resolve_redirect(
    page: Iterable[list],
    stats: Mapping,
    snapshot_title2id: Mapping,
    redirects_history: Mapping,
    count_recursive_calls: int) -> Iterator[list]:

    stats['performance']['redirects_analyzed'] += 1

    count_recursive_calls = count_recursive_calls + 1

    result = None
    page_id = int(page[0])
    page_title = page[1]

    if page_id in redirects_history:
        # page is a redirect
        redirect = redirects_history[page_id]
        target_title = redirects_history[page_id].target
        target_title = normalize_title(target_title)
        target_id = snapshot_title2id.get(target_title, None)

        if page_id == target_id:
            return page

        if target_title == '#NOREDIRECT':
            # page is not a redirect, return page as it is
            result = page

        else:
            if target_id is None:
                # target page not in snapshot
                result = [-1, '#DANGLINGREDIRECT', -1, -1, -1]
            else:
                # target page is in snapshot

                # page_id,
                # page_title,
                # revision_id,target_page
                # revision_parent_id,
                # revision_timestamp
                redrev = redirect.page.revision

                target_rev_id = redrev.id
                target_rev_parent_id = redrev.parent_id
                target_rev_timestamp = redrev.timestamp.isoformat()

                target_page = [target_id,
                               target_title,
                               target_rev_id,
                               target_rev_parent_id,
                               target_rev_timestamp
                               ]
                if count_recursive_calls <= MAX_RECURSION:
                    result = resolve_redirect(
                                page=target_page,
                                stats=stats,
                                snapshot_title2id=snapshot_title2id,
                                redirects_history=redirects_history,
                                count_recursive_calls=count_recursive_calls
                                )
                else:
                    import ipdb; ipdb.set_trace()

                # if int(target_id) != int(result[0]):
                #     import ipdb; ipdb.set_trace()
    else:
        # page is not a redirect, return page as it is
        result = page

    return result


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
            'redirects_analyzed': 0,
            'pages_analyzed': 0,
        }
    }
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    redirects = args.redirects
    inputfile_full_path = [afile for afile in args.files
                           if afile.name == basename][0]

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       (basename + '.resolve_redirect.features.csv'))
        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/
                    (basename + '.resolve_redirect.stats.xml')),
            compression=args.output_compression,
        )
    writer = csv.writer(pages_output)

    match = re_snapshotname.match(basename)
    if match:
        snapshot_date = arrow.get(match.group(1), 'YYYY-MM-DD')
    assert (snapshot_date > DATE_START and snapshot_date < DATE_NOW)
    # snapshot_date = snapshot_date.strftime('%Y-%m-%d')

    redirects_history = read_redirects(redirects, snapshot_date)

    snapshot_title2id = read_snapshot_pages(inputfile_full_path)
    
    dump = csv.reader(dump)
    pages_generator = process_lines(
        dump,
        stats,
        snapshot_title2id=snapshot_title2id,
        redirects_history=redirects_history
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
