import tpcarrow.dataset
import tpcarrow.tpc_ds
import tpcarrow.tpc_h
import tpcarrow.tpch_suite

import argparse


def cli(args=None):
    parser = argparse.ArgumentParser(
        description="This is a dataset generator wrapper/ converter to Arrow format for TPC-H/DS benchmarks. The project is still WIP, so there is no full support for all of the tables. Do create a issue for more.")
    parser.add_argument('vars', metavar='N', type=str, nargs='+',
                        help='an integer for the accumulator')
    parser.add_argument('--generate_table', nargs='+',
                        help='Generates table from dbgen runtime')
    print(parser.parse_args()._get_kwargs())
    args = parser.parse_args()
