import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText

from apache_beam.options.pipeline_options import PipelineOptions

input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_file = 'output/results.jsonl.gz'

options = PipelineOptions()
p = beam.Pipeline(options=options)

# csv_lines = (p | ReadFromText(input_file, skip_header_lines=1) | beam.io.WriteToText(output_file))
csv_lines = (p | ReadFromText(input_file) | beam.io.WriteToText(output_file))

print(csv_lines)

# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    pass

# See PyCharm help at https://www.jetbrains.com/help/pycharm/