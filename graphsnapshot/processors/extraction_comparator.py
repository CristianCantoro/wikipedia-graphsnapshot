"""
Compare extractions at two different dates.

The output format is csv.
"""

import os
import sys
import csv
import json
import glob
import mwxml
import arrow
import regex as re
import pathlib
import jsonable
import datetime
import itertools
import functools
import collections
from io import StringIO
from typing import (Iterable, Iterator, Mapping, NamedTuple, Optional)

import more_itertools

from .. import utils
from .. import file_utils as fu
from .. import dumper


NPRINTREVISION = 10000

DATE_START = arrow.get('2001-01-16', 'YYYY-MM')
DATE_NOW = arrow.now()

CHUNK_REGEXES = {}

# wikilink_chunk_regex:
#
# example filenames:
#   * enwiki-20150901-pages-meta-history1.xml-p000000010p000002861.7z.features.xml.gz
#   * svwiki-20180301-pages-meta-history.xml.7z.features.xml.gz
#
# 1: lang
# 2: date
# 3: historyno
# 4: pageid_first
# 5: pageid_last
# 6: dumpext
# 7: ext
wikilink_chunk_regex =  r'([a-z]{2})wiki-(\d{8})'
wikilink_chunk_regex += r'-pages-meta-history(\d{1,2})?\.xml'
wikilink_chunk_regex += r'(?:-p(\d+)p(\d+))?\.(gz|bz2|7z)'
wikilink_chunk_regex += r'\.features\.xml(?:\.[^\.]+)(?:\.(gz|bz2|7z))?'
re_wikilink_chunk = re.compile(wikilink_chunk_regex, re.IGNORECASE | re.DOTALL)


# linksnapshot_chunk_regex:
#
# example filenames:
#   * enwiki.link_snapshot.2001-03-01.csv.gz
#
# 1: lang
# 2: date
# 3: ext
linksnapshot_chunk_regex =  r'([a-z]{2})wiki\.'
linksnapshot_chunk_regex += r'link_snapshot\.(\d{4}-\d{2}-\d{2})\.csv'
linksnapshot_chunk_regex += r'(?:\.(gz|bz2|7z))?'
re_linksnapshot_chunk = re.compile(linksnapshot_chunk_regex, re.IGNORECASE | re.DOTALL)


CHUNK_REGEXES = {
'wikilink': re_wikilink_chunk,
'link-snapshots': re_linksnapshot_chunk,
}


PageData = NamedTuple('PageData', [
    ('id', int),
    ('timestamp', jsonable.Type),
    ('data', dict),
])


# - Chunk:
#   - lang
#   - date
#   - historyno
#   - pageid_first
#   - pageid_last
#   - dumpext
#   - ext
Chunkfile = NamedTuple('Chunkfile', [
    ('lang', str),
    ('date', str),
    ('historyno', int),
    ('pageid_first', int),
    ('pageid_last', int),
    ('dumpext', str),
    ('ext', str),
])


stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <new>
            <pages_analyzed>${stats['performance']['new']['pages_analyzed'] | x}</pages_analyzed>
            <revisions_analyzed>${stats['performance']['new']['revisions_analyzed'] | x}</revisions_analyzed>
        </new>
        <old>
            <pages_analyzed>${stats['performance']['old']['pages_analyzed'] | x}</pages_analyzed>
            <revisions_analyzed>${stats['performance']['old']['revisions_analyzed'] | x}</revisions_analyzed>
        </old>
    </performance>
</stats>
'''


# Look ahead one element in a Python generator
# https://stackoverflow.com/a/2425347/2377454
def peek_generator(g):
    peek = next(g)
    return peek, itertools.chain([peek], g)


def progress(what: Optional[str]='.') -> None:
    print(what, end='', file=sys.stderr, flush=True)


def sort_revisions(rev):
    return (rev[1].timestamp, rev[0])


def process_pages(dump: Iterable[list],
                  header: Iterable[list],
                  stats: Mapping,
                  max_timestamp: arrow.arrow.Arrow,
                  only_last_revision: Optional[bool]=False,
                  which: Optional[str]='old') -> Iterator[list]:

    line = None
    prevline = None
    is_last_line = None

    page = None
    prevpage = None

    revisions = []
    sorted_revisions = None

    counter = 0
    lineno = 1

    while True:

        prevline = line
        prevpage = page

        is_last_line = False
        line = next(dump, None)
        lineno = lineno + 1

        if line is None:
            line = prevline
            is_last_line = True

        # read the line in a StringIO object and parse it with the csv module
        try:
            parsed = [l for l in csv.reader(StringIO(line))][0]
        except (csv.Error, TypeError) as err:
            return None

        data = dict(zip(header, parsed))
        # PageData = NamedTuple('PageData', [
        #     ('id', int),
        #     ('timestamp', jsonable.Type),
        #     ('data', dict),
        # ])
        try:
            pid = int(data['page_id'])
        except:
            pid = -1

        try:
            revtimestamp = arrow.get(data['revision_timestamp'])
        except:
            # set timestamp to EPOCH
            revtimestamp = arrow.get(0)

        page = PageData(pid,revtimestamp,data)

        if prevpage is None:
            if which == 'old':
                # utils.log("Processing < {title} {{id:{id}}} "
                #           .format(title=page.title, id=page.id))
                utils.log("Processing < {title} {{id:{id}}} "
                          .format(title=page.data['page_title'],
                                  id=page.id
                                  )
                          )

            else:
                # utils.log("Processing > {title} {{id:{id}}} "
                #           .format(title=page.title, id=page.id))
                utils.log("Processing > {title} {{id:{id}}} "
                          .format(title=page.data['page_title'],
                                  id=page.id
                                  )
                          )

        if prevpage is None or prevpage.id != page.id:
            # we are starting now or we have a new page
            counter = 0
            stats['pages_analyzed'] += 1

        # if prevpage is None or \
        #         prevpage.revision.id != page.revision.id
        if prevpage is None or \
                int(prevpage.data['revision_id']) != int(page.data['revision_id']):
            stats['revisions_analyzed'] += 1

        if not is_last_line and \
                (prevpage is None or prevpage.id == page.id):
            # it is not the last revision, futhermore two cases:
            #   * prevpage is None: we are reading the first line of the
            #     dump file
            #   * dump_prevpage.id == dump_page.id we are reading a page whose
            #     is is the same as the previous one we read
            # so, either we just started or we are in the middle of the
            # history of a page. What we do is we just add the revision to the
            # list.

            counter = counter + 1

            if counter % NPRINTREVISION == 1:
                if which == 'old':
                    progress('.')
                else:
                    progress(':')

            if page.timestamp <= max_timestamp:
                revisions.append( (lineno, page) )

            prevpage = page

        else:
            if is_last_line:
                yield None
            # cases:
            #   * this is the last revision of the dump
            #     (is_last_line is True)
            #   * we have changed to a new page (dump_prevpage is not None
            #     and dump_prevpage.id != dump_page.id)

            # sort all the revision by timestamp (they are not guaranted to be
            # ordered)
            sorted_revisions = sorted(revisions, key=sort_revisions)

            # if we only want the sorted_revisions list is limited to the
            # last element.

            # Note: don't try to bee to smart and think that one can skip
            # reading all revisions and just take the last that is encountered,
            # that is when page.id changes, i.e.
            #     when dump_prevpage.id != dump_page.id
            # because as said in the previous comment we are not assured that
            # all revisions will be in the correct order, so we still need to
            # collect them all, sort them and take the last one.
            if only_last_revision:
                sorted_revisions = [sorted_revisions[-1]]

            yield sorted_revisions
            del sorted_revisions

            if prevpage.id != page.id:
                if which == 'old':
                    # utils.log("Processing < {title} {{id:{id}}} "
                    #           .format(title=page.title, id=page.id))
                    utils.log("Processing < {title} {{id:{id}}} "
                              .format(title=page.data['page_title'],
                                      id=page.id
                                      )
                              )

                else:
                    # utils.log("Processing > {title} {{id:{id}}} "
                    #           .format(title=page.title, id=page.id))
                    utils.log("Processing > {title} {{id:{id}}} "
                              .format(title=page.data['page_title'],
                                      id=page.id
                                      )
                              )

            # we are not interested to the new page for the moment, put it in
            # the list
            prevpage = page
            revisions = [ (lineno, page) ]


def get_header(dump: Iterable[list]) -> Iterable[list]:
    hline = next(dump)
    header = [l for l in csv.reader(StringIO(hline))][0]

    return header 

def equal_dicts(d1, d2, keys_to_compare):
    keys = set(keys_to_compare)
    for k1, v1 in d1.items():
        if k1 not in keys:
            continue

        if (k1 not in d2 or d2[k1] != v1):
            return False

    for k2, v2 in d2.items():
        if k2 not in keys:
            continue

        if k2 in keys and k2 not in d1:
            return False

    return True


def compare_data(old_data: Mapping,
                 new_data: Mapping,
                 exclude_columns: Iterable[list],
                 all_columns: bool) -> bool:

    old_keys = set(old_data.keys())
    new_keys = set(new_data.keys())

    columns_to_compare = set()
    if all_columns:
        # take all the columns
        columns_to_compare = old_keys.union(new_keys)
    else:
        # take only the common columns
        columns_to_compare = old_keys.intersection(new_keys)

    if exclude_columns:
        columns_to_compare = columns_to_compare.difference(exclude_columns)

    return equal_dicts(old_data, new_data, columns_to_compare)


def compare_pages(old_hist: Iterable[list],
                  new_hist: Iterable[list],
                  header_old: Iterable[list],
                  header_new: Iterable[list],
                  all_columns: bool,
                  exclude_columns: Iterable[list]) -> Iterable[list]:

    utils.log('<{old} ({nrevold}), {new} ({nrevnew})> '
              .format(old=old_hist[0][1].data['page_title'],
                      new=new_hist[0][1].data['page_title'],
                      nrevold=len(old_hist),
                      nrevnew=len(new_hist)
                      )
              )

    i = 0
    j = 0

    equal_count = 0
    mod_count = 0
    add_count = 0
    sub_count = 0
    diff = []
    while (i < len(old_hist) or j < len(new_hist)):
        lineno, old = old_hist[i] if i < len(old_hist) else (None, None)
        lineno, new = new_hist[j] if j < len(new_hist) else (None, None)

        if old and i % NPRINTREVISION == 0:
            print('.', end='', file=sys.stderr, flush=True)
        if new and j % NPRINTREVISION == 0:
            print(':', end='', file=sys.stderr, flush=True)

        if old and new:
            if compare_data(old_data=old.data,
                            new_data=new.data,
                            all_columns=all_columns,
                            exclude_columns=exclude_columns):
                # old == new:
                equal_count = equal_count + 1
                # --- if old and new and old == new

            else:
                # old != new:
                mod_count = mod_count + 1

                diff.append( ('-', lineno, old.data) )
                diff.append( ('+', lineno, new.data) )
                # --- if old and new and old != new

        if new and old is None:
            add_count = add_count + 1

            diff.append( ('+', lineno, new.data) )
            # --- if new and old is None

        if old and new is None:
            sub_count = sub_count + 1

            diff.append( ('-', lineno, old.data) )
            # --- if old and new is None

        i = i + 1
        j = j + 1

    # print string
    diff_string = ('={equal},~{mod},-{sub},+{add}'
                   .format(equal=equal_count,
                           mod=mod_count,
                           add=add_count,
                           sub=sub_count)
                   )
    print(' ({})'.format(diff_string), file=sys.stderr, flush=True)

    return diff


def process_dumps(
        old_dump: Iterable[list],
        stats: Mapping,
        selected_chunks: Iterable[list],
        max_timestamp: arrow.arrow.Arrow,
        header_old: Iterable[list],
        header_new: Iterable[list],
        all_columns: bool,
        exclude_columns: Iterable[list],
        only_last_revision: bool=False) -> Iterator[list]:
    """Compare revisions in `old_dump` with revisions from `selected_chunks`.
    """

    new_dump = itertools.chain.from_iterable([afile
                for afile in map(fu.open_csv_file, selected_chunks)])
    # skip header
    next(new_dump)

    old_generator = process_pages(dump=old_dump,
                                  header=header_old,
                                  stats=stats['performance']['old'],
                                  max_timestamp=max_timestamp,
                                  only_last_revision=only_last_revision,
                                  which='old'
                                  )

    new_generator = process_pages(dump=new_dump,
                                  header=header_new,
                                  stats=stats['performance']['new'],
                                  max_timestamp=max_timestamp,
                                  only_last_revision=only_last_revision,
                                  which='new'
                                  )

    read_old = True
    read_new = True
    break_flag = False
    count = 0
    while True:
        if break_flag:
            break

        if read_old:
            # oldpagehist = [(line, page) for line, page in next(old_generator)
            #                if page.revision.timestamp <= max_timestamp]
            oldpagehist = next(old_generator)
            old_head = oldpagehist[0]


        if read_new:
            # newpagehist = [(line, page) for line, page in next(new_generator)
            #                if page.revision.timestamp <= max_timestamp]
            newpagehist = next(new_generator)
            new_head = newpagehist[0]

        old_pageid = old_head[1].id
        new_pageid = new_head[1].id

        old_pagetitle = old_head[1].data['page_title']
        new_pagetitle = new_head[1].data['page_title']

        if old_pageid < new_pageid:
            symbol = '<'

            read_new = False
            read_old = True

        elif old_pageid > new_pageid:
            symbol = '>'

            read_new = True
            read_old = False

        else:
            symbol = '=='

            read_new = True
            read_old = True

        if old_pageid != new_pageid:
            utils.log(('Do not compare '
                       '{old_title} ({old_id}) '
                       '{symbol} '
                       '{new_title} ({new_id})\n'
                       ).format(old_title=old_pagetitle,
                                old_id=old_pageid,
                                symbol=symbol,
                                new_title=new_pagetitle,
                                new_id=new_pageid,
                                )
                      )
        else:
            # compare_pages(old_hist, new_hist, header_old, header_new)
            difflist = compare_pages(old_hist=oldpagehist,
                                     new_hist=newpagehist,
                                     header_old=header_old,
                                     header_new=header_new,
                                     all_columns=all_columns,
                                     exclude_columns=exclude_columns,
                                     )

            yield difflist

        oldpagenext, old_generator = peek_generator(old_generator)
        newpagenext, new_generator = peek_generator(new_generator)

        if oldpagenext is None:
            read_old = False

        if newpagenext is None:
            read_new = False

        # if we do not need to read neither the new, nor the old file then
        # we are finished
        if read_old is False and read_new is False:
            break_flag = True

        if old_pageid == 2114 and new_pageid == 2114:
            count += 1

        if count > 1:
            import ipdb; ipdb.set_trace()


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    def is_dir(value):
        path_value = pathlib.Path(value)
        if not path_value.is_dir():
            import argparse
            raise argparse.ArgumentTypeError(
                "{} is not a directory".format(value))

        return path_value


    parser = subparsers.add_parser(
        'compare-extractions',
        help='Compare extractions.',
    )

    parser.add_argument(
        '--new-extractions-dir',
        type=is_dir,
        required=True,
        help='Directory with the new extractions.'
    )

    parser.add_argument(
        '--extractions-type',
        type=str,
        required=True,
        choices=list(CHUNK_REGEXES.keys()),
        help='Type of extractions.'
    )

    parser.add_argument(
        '--new-chunks',
        type=str,
        nargs='+',
        default=None,
        help='New chunks [default: infer from input].'
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

    parser.add_argument(
        '--header-old',
        type=str,
        help='List of comma-separated column names to compare from '
             'the old extraction '
             '[default: use all columns, reading from the first line of the'
             ' file, and compare only the ones that appear in both files].'
    )
    parser.add_argument(
        '--header-new',
        type=str,
        help='List of comma-separated column names to compare from '
             'the new extractions '
             '[default: use all columns, reading from the first line of the'
             ' file, and compare only the ones that appear in both files].'
    )
    parser.add_argument(
        '--all-columns',
        action='store_true',
        help='Consider all the columns of the files, except the ones listed '
             'in --exclude-columns. The default behaviour is to compare only '
             'the common column.'
    )
    parser.add_argument(
        '-x',
        '--exclude-columns',
        type=str,
        help='List of comma-separated column names to exclude from the '
             'comparison.'
    )
    parser.add_argument(
        '--only-last-revision',
        action='store_true',
        help='Consider only the last revision for each page.'
    )

    parser.set_defaults(func=main)


def idtoint(pageid):
    return int(pageid.lstrip('0'))


def extract_name_elements(match, ext_type):
    res = None
    if ext_type == 'wikilink':
        # wikilink_chunk_regex:
        # 1: lang
        # 2: date
        # 3: historyno
        # 4: pageid_first
        # 5: pageid_last
        # 6: dumpext
        # 7: ext
        lang = match.group(1)
        date = match.group(2)
        historyno = (idtoint(match.group(3))
                     if match.group(3)
                     else ''
                     )
        pageid_first = (idtoint(match.group(4))
                        if match.group(4)
                        else ''
                        )
        pageid_last = (idtoint(match.group(5))
                       if match.group(5)
                       else ''
                       )
        dumpext = match.group(6)
        ext = match.group(7) or ''

        res = Chunkfile(lang,
                        date,
                        historyno,
                        pageid_first,
                        pageid_last,
                        dumpext,
                        ext)

    elif ext_type == 'link-snapshots':
        # linksnapshot_chunk_regex:
        # 1: lang
        # 2: date
        # 3: ext
        lang = match.group(1)
        date = match.group(2)
        ext = match.group(3) or ''

        res = Chunkfile(lang,
                        date,
                        None,
                        -1,
                        -1,
                        None,
                        ext)

    return res


def select_intervals(old_interval, new_intervals_list):
    old_start = old_interval[0]
    old_end = old_interval[1]

    intervals = set()
    for start, end in new_intervals_list:
        if (start <= old_start and end >= old_start) or \
                (start <= old_start and end >= old_start) or \
                (end >= old_end and start <= old_end):
            intervals.add((start, end))

    return intervals


def sort_chunks(chunk_path):
    chunk_basename = os.path.basename(chunk_path)
    match = re_chunk.match(chunk_basename)
    achunk = extract_name_elements(match)

    return achunk.pageid_first


def select_chunks(base_chunk, args):
    # new chunk glob
    #   * enwiki-20180301-pages-meta-history1.xml-p10p2115.7z.features.xml.gz
    #     {lang}wiki-{date}-pages-meta-history{historyno}.xml
    #       -p{pageid_first}p{pageid_last}.{dumpext}.features.xml.{ext}
    new_chunk_pattern =  '{lang}wiki-{date}'
    new_chunk_pattern += '-pages-meta-history{historyno}.xml'
    new_chunk_pattern += '-p{pageid_first}p{pageid_last}.{dumpext}'
    new_chunk_pattern += '.features.xml.{ext}'

    if not base_chunk.historyno:
        new_chunk_pattern = new_chunk_pattern.replace(r'{historyno}', '')

    if not base_chunk.pageid_first or not base_chunk.pageid_first:
        new_chunk_pattern = (new_chunk_pattern
                             .replace(r'-p{pageid_first}p{pageid_last}', '')
                             )

    # {lang}wiki-{date}-pages-meta-history{historyno}.xml
    #   -p{pageid_first}p{pageid_last}.{dumpext}.features.xml.{ext}
    new_chunk_glob = (new_chunk_pattern
                      .format(lang=base_chunk.lang,
                               date='*',
                               historyno='*',
                               pageid_first='*',
                               pageid_last='*',
                               dumpext='*',
                               ext='*')
                      )

    new_chunks_ids = []
    new_chunks_dates = set()

    glob_path = os.path.join(new_extractions_dir.as_posix(),
                             new_chunk_glob
                             )
    for cf in glob.glob(glob_path):

        cf_basename = os.path.basename(cf)
        new_match = re_chunk.match(cf_basename)
        if new_match:
            new_chunk = extract_name_elements(new_match)
            new_chunks_ids.append((new_chunk.pageid_first,
                                  new_chunk.pageid_last)
                                 )
            new_chunks_dates.add(new_chunk.date)
        else:
            msg = ("Unexpected name for new chunk: {filename} ({path})"
                   .format(filename=cf_basename,
                           path=cf))
            raise ValueError(msg)
            del msg
    new_chunks_ids.sort()

    if args.new_extraction_date is None:
        if len(new_chunks_dates) > 1:
            msg = ("Multiple dates extracted from new chunks filenames")
            raise ValueError(msg)
            del msg

        new_date = new_chunks_dates.pop()
        new_extraction_date = arrow.get(new_date, 'YYYYMMDD')
    else:
        new_extraction_date = arrow.get(args.new_extraction_date)

    assert (new_extraction_date > DATE_START and \
                new_extraction_date < DATE_NOW)

    selected_intervals = select_intervals(
        (base_chunk.pageid_first, base_chunk.pageid_last),
        new_chunks_ids
        )

    new_chunk_glob = (new_chunk_pattern
                      .format(lang=base_chunk.lang,
                               date=new_extraction_date.format('YYYYMMDD'),
                               historyno='*',
                               pageid_first='*',
                               pageid_last='*',
                               dumpext='*',
                               ext='*')
                      )

    selected_chunks = set()
    for cf in glob.glob(os.path.join(new_extractions_dir.as_posix(),
                                     new_chunk_glob
                                     )):
        cf_basename = os.path.basename(cf)
        new_match = re_chunk.match(cf_basename)
        if new_match:
            new_chunk = extract_name_elements(new_match)
            if (new_chunk.pageid_first, new_chunk.pageid_last) in \
                    selected_intervals:
                selected_chunks.add(cf)

    selected_chunks = sorted(selected_chunks, key=sort_chunks)

    return selected_chunks


def main(
        dump: Iterable[list],
        basename: str,
        args) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'old': {
                'pages_analyzed': 0,
                'revisions_analyzed': 0,
                'lines': 0
                },
            'new': {
                'pages_analyzed': 0,
                'revisions_analyzed': 0,
                'lines': 0
            }
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

    re_chunk = CHUNK_REGEXES[args.extractions_type]
    base_match = re_chunk.match(basename)

    if base_match:
        base_chunk = extract_name_elements(match=base_match,
                                           ext_type=args.extractions_type)
    else:
        msg = ("Unexpected name for input file: {filename} ({path})"
               .format(filename=basename, path=inputfile_full_path))
        raise ValueError(msg)
        del msg

    if args.old_extraction_date is None:
        old_extraction_date = arrow.get(base_chunk.date, 'YYYYMMDD')
    else:
        old_extraction_date = arrow.get(args.old_extraction_date)

    assert (old_extraction_date > DATE_START and \
                old_extraction_date < DATE_NOW)

    selected_chunks = []
    if not args.new_chunks:
        if args.extractions_type == 'wikilinks':
            selected_chunks = select_chunks(base_chunk, args)
        else:
            raise NotImplementedError(
                'Can not handle chunk selection automatically'
                'for extractions of type {}.'
                .format(args.extractions_type)
                )
    else:
        selected_chunks = [str(args.new_extractions_dir/chunk)
                           for chunk in args.new_chunks
                           ]

    utils.log("Comparing with select chunks:")
    for chunk in selected_chunks:
        utils.log("  * {}.".format(chunk))
    utils.log("---")

    header_old = args.header_old.split(',') if args.header_old else None
    header_new = args.header_new.split(',') if args.header_new else None

    if header_old is None:
        header_old = get_header(dump)

    if header_new is None:
        header_new = get_header(fu.open_csv_file(selected_chunks[0]))

    ignore_newer_than = DATE_NOW
    if ignore_newer:
        ignore_newer_than = old_extraction_date

    exclude_columns = (args.exclude_columns.split(',')
                       if args.exclude_columns else None)

    difflist_generator = process_dumps(
        dump,
        stats,
        selected_chunks=selected_chunks,
        max_timestamp=ignore_newer_than,
        header_old=header_old,
        header_new=header_new,
        all_columns=args.all_columns,
        exclude_columns=exclude_columns,
        only_last_revision=args.only_last_revision,
        )

    one_header = False
    if not (set(header_old) - set(header_new)):
        one_header = True
        writer = csv.DictWriter(pages_output,
                                fieldnames=['change', 'lineno']+header_old)
    else:
        old_fields = ['old.{}'.format(field)
                      for field in header_old]
        new_fields = ['new.{}'.format(field)
                      for field in header_new]
        writer = csv.DictWriter(
            pages_output,
            fieldnames=['change', 'lineno']+old_fields+new_fields)

    writer.writeheader()

    for difflist in difflist_generator:
        for diff in difflist:
            change = diff[0]
            lineno = diff[1]
            changedata = diff[2]

            if one_header:
                data = {'change': change,
                        'lineno': lineno,
                        **changedata
                        }
            else:
                old_fields = [('old.{}'.format(key), changedata[key])
                              for key in changedatachangedata if key in header_old
                              ]
                new_fields = [('new.{}'.format(key), changedata[key])
                              for key in changedata if key in header_new
                              ]

                data = dict(old_fields+new_fields)

            writer.writerow(data)

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
