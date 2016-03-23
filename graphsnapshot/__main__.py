"""Main module that parses command line arguments."""
import argparse
import pathlib
import csv

from . import processors, utils, file_utils


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog='graphsnapshot',
        description='Graph snapshot features extractor.',
    )
    parser.add_argument(
        'files',
        metavar='FILE',
        type=pathlib.Path,
        nargs='+',
        help='XML Wikidump file to parse. It accepts only 7z.',
    )
    parser.add_argument(
        'output_dir_path',
        metavar='OUTPUT_DIR',
        type=pathlib.Path,
        help='XML output directory.',
    )
    parser.add_argument(
        '--output-compression',
        choices={None, '7z', 'gzip'},
        required=False,
        default=None,
        help='Output compression format.',
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help="Don't write any file",
    )

    subparsers = parser.add_subparsers(help='sub-commands help')
    processors.snapshot_extractor.configure_subparsers(subparsers)
    processors.link_snapshot_extractor.configure_subparsers(subparsers)

    parsed_args = parser.parse_args()
    if 'func' not in parsed_args:
        parser.print_usage()
        parser.exit(1)

    return parsed_args


def main():
    """Main function."""
    args = get_args()

    if not args.output_dir_path.exists():
        args.output_dir_path.mkdir(parents=True)

    for input_file_path in args.files:
        utils.log("Analyzing {}...".format(input_file_path))

        dump = csv.reader(file_utils.open_csv_file(str(input_file_path)))

        basename = input_file_path.name

        args.func(
            dump,
            basename,
            args,
        )


if __name__ == '__main__':
    main()
