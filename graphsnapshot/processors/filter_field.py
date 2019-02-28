"""
Extract links from list of revisions.

The output format is csv.
"""

import io
import sys
import csv
import json
import collections
import datetime
import functools

import jsonable
import more_itertools
import mwxml
import regex
import arrow
from typing import Iterable, Iterator, Mapping, NamedTuple, Optional

from .. import utils
from .. import file_utils as fu
from .. import dumper


stats_template = \
'''<stats>
    <performance>
        <start_time>${stats['performance']['start_time']}</start_time>
        <end_time>${stats['performance']['end_time']}</end_time>
        <revisions_analyzed>${stats['performance']['revisions_analyzed']}</revisions_analyzed>
    </performance>
    <filter>
        <lines>${stats['filter']['lines']}</lines>
    </filter>
</stats>
'''


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        header: Iterator[list],
        filter_regexes: Mapping,
        replace_regexes: Mapping) -> Iterator[list]:
    """Assign each revision to the snapshot to which they
       belong.
    """

    old_linkline = None
    linkline = None

    page_id = None
    page_rev_id = None

    prevpage_id = None
    prevpage_rev_id = None
    # -------------------------------------------------------------------------
    # OUTLINE OF THE ALGORITHM
    #
    # * read a line at a time from the input file, we call each line a linkline
    #   (because each line of the input has a wikilink)
    #
    # * if the linkline has a page and revision id that are contained in the
    #   snapshot process them, otherwise skip
    # -------------------------------------------------------------------------

    # Loop over all lines, this is equivalent to
    # for link in dump:
    while True:
        old_linkline = linkline
        linkline = next(dump, None)

        if linkline is None:
            # this is the last line, end loop.
            break

        page_data = dict(zip(header, linkline))
        page_id = int(page_data['page_id'])
        page_rev_id = int(page_data['revision_id'] )

        # The code below code is executed when we encounter a new page id for
        # the first time.
        if prevpage_id is None or prevpage_id != page_id:
            utils.log("Processing page id {}".format(page_data['page_id']))
            stats['performance']['pages_analyzed'] += 1

        if prevpage_rev_id is None or prevpage_rev_id != page_rev_id:
            stats['performance']['revisions_analyzed'] += 1

        # if page_data['page_title'] == "V8 engine" \
        #        and 'see also' in page_data['wikilink.section_name'].lower():
        #    import ipdb; ipdb.set_trace()

        field_data = {}
        for field, re_replaces in replace_regexes.items():
            field_data[field] = page_data[field]
            for areplace in re_replaces:
                # re.sub(pattern, repl, string, count=0, flags=0)
                # https://docs.python.org/3/library/re.html#re.sub
                #
                # if compiled:
                #   re.sub(repl, string)
                # repl, goes before string
                field_data[field] = areplace['pattern'].sub(
                    areplace['repl'],
                    field_data[field],
                    )

        # if page_data['page_title'] == "V8 engine" \
        #        and 'see also' in page_data['wikilink.section_name'].lower():
        #    import ipdb; ipdb.set_trace()

        select_line = True
        for field, re_filters in filter_regexes.items():
            # conditions are in conjuction
            select_line = True
            for afilter in re_filters:
                # filters are in disjuntion
                amatch = afilter.match(field_data[field])
                if amatch is None:
                    select_line = False
                    break

        # if page_data['page_title'] == "V8 engine" \
        #        and 'see also' in page_data['wikilink.section_name'].lower():
        #    import ipdb; ipdb.set_trace()


        # Print pagetitle for each different revision analyzed,
        # that is at most once.
        if prevpage_id != page_id:
            # we print the page title in parenthesys
            page_title = " ({})".format(page_data['page_title'])
            print(page_title, end='', file=sys.stderr)

        # print a dot for each link analyzed
        utils.dot()

        if select_line:
            stats['filter']['lines'] += 1
            yield page_data

        prevpage_id = page_id
        prevpage_rev_id = page_rev_id


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'filter-field',
        help='Filter files by field value',
    )
    parser.add_argument(
        '--filter',
        required=True,
        type=str,
        help='File containing the list of fields and values to match.'
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
        'filter': {
          'lines': 0
        }
    }

    with open(args.filter, 'r') as f:
        _filter = json.load(f)

    filter_regexes = collections.defaultdict(list)
    replace_regexes = collections.defaultdict(list)
    field = _filter['field']
    for afilter in _filter['filter']:
        if afilter['type'] == 'regex':
            filter_regexes[field].append(regex.compile(afilter['pattern']))
        else:
            raise NotImplementedError(
                "The only type of filter implemented at the moment 'regex'")

    for areplace in _filter['replace']:
        if afilter['type'] == 'regex':
            replace_regexes[field].append(
                {'pattern': regex.compile(areplace['pattern']),
                 'repl': areplace['repl']
                 }
                )
        else:
            raise NotImplementedError(
                "The only type of replace implemented at the moment 'regex'")


    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       (basename + '.features.csv'))

        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/(basename + '.stats.xml')),
            compression=args.output_compression,
        )

    with pages_output:
        stats['performance']['start_time'] = datetime.datetime.utcnow()

        dump = csv.reader(dump)

        # get header
        header = next(dump)

        pages_generator = process_lines(
            dump,
            stats,
            header=header,
            filter_regexes=filter_regexes,
            replace_regexes=replace_regexes,
        )

        writer = csv.DictWriter(pages_output, fieldnames=header)
        writer.writeheader()
        for page_data in pages_generator:
            writer.writerow(page_data)
        stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
