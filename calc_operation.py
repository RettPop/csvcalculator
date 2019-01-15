#!/usr/bin/env python
import csv
import sys
import os
import re
from datetime import datetime, timedelta
from dateutil.parser import parse
import argparse
from locale import setlocale
from locale import LC_NUMERIC
from locale import atof

NO_PERIOD_ID = "no"

FIELD_NAME_PERIOD = "during_period"

FIELD_NAME_OPERATION = "operation"

NO_PERIODS_SECTION = "base"

PERIODS_SECTION = "period"

PERIODS_SINGLE_NAME = "yes"


def start_read():
    args = handle_command_line(sys.argv)
    input_file_name = args.input_file
    periods_file_name = args.periods_file
    out_file_name = args.output_file
    periods = read_periods(periods_file_name, None if args.separate_periods else PERIODS_SINGLE_NAME)
    setlocale(LC_NUMERIC, '')

    with open(input_file_name) as csv_file:
        dialect = csv.Sniffer().sniff(csv_file.read(4096))
        csv_file.seek(0)
        reader = csv.DictReader(csv_file, dialect=dialect)

        csv_fields = reader.fieldnames
        results_during_period = {}
        # results_out_of_period = {}

        # periods_id_list = set()

        # for per in periods:
        #     periods_id_list.add(per["id"])
        #
        # periods_id_list.add(NO_PERIOD_ID)

        # for per_id in periods_id_list:
        #     results_during_period[per_id] = {}
        #     for fld in csv_fields:
        #         results_during_period[per_id][fld] = []

        operations = []
        if args.sum:
            operations.append("sum")

        if args.averages:
            operations.append("avg")

        # differentiating data by if it fits any period or not
        for row in reader:
            in_period = NO_PERIOD_ID
            row_time = parse(row['Time'])
            for period in periods:
                if row_time >= period['start'] and row_time <= period['end']:
                    in_period = period['id']
                    break

            for fld in csv_fields:
                try:
                    num = atof(row[fld])
                except:
                    num = "NaN"

                if in_period not in results_during_period.keys():
                    results_during_period[in_period] = {}

                if fld not in results_during_period[in_period].keys():
                    results_during_period[in_period][fld] = []

                results_during_period[in_period][fld].append(num)

        # set mandatory fields for arrays
        def prepare_results_array(operation, periods_id_list):
            result_array = {}
            for per in periods_id_list:
                result_array[per] = {}
                result_array[per][FIELD_NAME_PERIOD] = per
                result_array[per][FIELD_NAME_OPERATION] = operation
            return result_array

        results = {}

        for op in operations:
            results[op] = prepare_results_array(operation=op, periods_id_list=results_during_period.keys())

        def perform_math_operation(operation, values_list):
            if "sum" == operation:
                return calc_list_sum(values_list)

            if "avg" == operation:
                return calc_list_avg(values_list)

        # performing required math operation
        for fld in csv_fields:
            for op in operations:
                for per in results_during_period.keys():
                    results[op][per][fld] = perform_math_operation(op, results_during_period[per][fld])

        # how many items we processed
        for per in results_during_period.keys():
            col_name = results_during_period[per].keys()[0]
            nr_items = len(results_during_period[per][col_name])
            for op in operations:
                results[op][per]["items"] = nr_items

        # writing results
        one_op = results.keys()[0]
        one_period = results[one_op].keys()[0]
        result_fields = results[one_op][one_period].keys()
        if out_file_name:
            out_file = open(out_file_name, "w")
            writer = csv.DictWriter(out_file, fieldnames=result_fields, dialect=dialect)
            writer.writeheader()
        else:
            writer = None
            print(";".join(result_fields))

        for op in results.keys():
            for per in results[op].keys():
                if writer:
                    writer.writerow(results[op][per])
                else:
                    print(";".join(map(str, results[op][per].values())))

        if writer:
            out_file.close()

        csv_file.close()


def calc_list_sum(list):
    result = "NaN"
    if len(list) > 0:
        try:
            result = sum(list)
        except TypeError:
            result = "NaN"

    return result


def calc_list_avg(list):
    result = "NaN"
    if len(list) > 0:
        try:
            result = float(sum(list)) / float(len(list))
        except TypeError:
            result = "NaN"

    return result


def read_periods(periods_file_name, single_name):
    with open(periods_file_name) as csv_file:
        reader = csv.DictReader(csv_file, delimiter=";")
        periods = []
        for row in reader:
            period_id = row['id'] if single_name is None else single_name
            periods.append({'start': parse(row['started']), 'end': parse(row['finished']), 'id': period_id})
    return periods


def handle_command_line(args):
    args_parser = argparse.ArgumentParser(description='Calculate sum or average of columns of CSV file with periodical rows')
    args_parser.add_argument('-i', '--input_file', action='store', dest='input_file', required=True)
    args_parser.add_argument('-o', '--output_file', action='store', dest='output_file', required=False)
    args_parser.add_argument('-p', '--periods_file', action='store', dest='periods_file', required=True)
    args_parser.add_argument('-r', '--separate-periods', action='store_true', dest='separate_periods', help="Do not consider all periods as with same id", default=False)
    args_parser.add_argument('-s', '--sum', action='store_true', dest='sum', help="calculate summ")
    args_parser.add_argument('-a', '--avg', action='store_true', dest='averages', help="calculate average")
    args = args_parser.parse_args()
    if not (args.sum or args.averages):
        args_parser.error('At least one math operation is required')

    return args


if __name__ == '__main__':
    # if len(sys.argv) < 4:
    #     print "Usage: " + os.path.basename(__file__) + " <stats filename> <periods filename> <out filename>"
    #     sys.exit(1)
    start_read()
