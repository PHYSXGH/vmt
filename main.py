from csv import DictWriter
from csv import excel
from io import StringIO

import apache_beam as beam

from datetime import datetime

from apache_beam.io.filesystem import CompressionTypes


"""
The solutions for both questions are in this file
Some functions that were used as bean.DoFn were sourced from stackoverflow, these are indicated in the comments

The testing uses pytest, and while it's not very extensive, it does test all of the filtering steps

I have excluded the origin and destination fields in the very beginning, as they don't seem to be needed
later on. This doesn't make much of a difference here, but should somewhat reduce memory consumption with larger datasets

The only feature that's missing is the names of the columns in the output, as I couldn't find the beam function to add it
and I didn't want to modify the saved file outside the pipeline
"""


input_file_name = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_file_name = 'output/results'


class DataRow(beam.DoFn):
    def process(self, element):
        timestamp, origin, destination, transaction_amount = element.split(",")
        return [
            beam.Row(
                timestamp=datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z").date(),
                # origin=origin,
                # destination=destination,
                transaction_amount=float(transaction_amount)
            )
        ]


class RowsToDict(beam.DoFn):
    def process(self, element):
        return [
            {
                'date': element.key,
                'total_amount': element.total_amount
            }
        ]


# sourced from https://stackoverflow.com/questions/46794351/google-cloud-dataflow-write-to-csv-from-dictionary
def _dict_to_csv(element, column_order, missing_val='', discard_extras=True, dialect=excel):
    """ Additional properties for delimiters, escape chars, etc via an instance of csv.Dialect
        Note: This implementation does not support unicode
    """

    buf = StringIO()

    writer = DictWriter(buf,
                        fieldnames=column_order,
                        restval=missing_val,
                        extrasaction=('ignore' if discard_extras else 'raise'),
                        dialect=dialect)
    writer.writerow(element)

    return buf.getvalue().rstrip(dialect.lineterminator)


# sourced from https://stackoverflow.com/questions/46794351/google-cloud-dataflow-write-to-csv-from-dictionary
class DictToCSVFn(beam.DoFn):
    """ Converts a Dictionary to a CSV-formatted String

        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in the input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """

    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def process(self, element, *args, **kwargs):
        result = _dict_to_csv(element,
                              column_order=self._column_order,
                              missing_val=self._missing_val,
                              discard_extras=self._discard_extras,
                              dialect=self._dialect)

        return [result, ]


# this is the composite transform for Task 2 that does most of the work except loading and saving the file
# as I thought those should be part of the main pipeline and only the transformation logic should be here
class CalculateDailyTransactions(beam.PTransform):
    def expand(self, pcoll):
        return(
            pcoll
            | 'Create a dict from the CSV' >> beam.ParDo(DataRow())
            | 'Select transactions with values greater than 20' >>
            beam.Filter(lambda entry: entry.transaction_amount > 20)
            | 'Remove transactions from before 2010' >>
            beam.Filter(lambda entry: entry.timestamp > datetime.strptime("2010-01-01", "%Y-%m-%d").date())
            | 'Group transactions by date' >> beam.GroupBy(lambda s: str(s.timestamp)).aggregate_field(
            'transaction_amount', sum, 'total_amount')
            | 'Create dictionary' >> beam.ParDo(RowsToDict())
            | 'Format dictionary as CSV' >> beam.ParDo(DictToCSVFn(['date', 'total_amount']))
        )


# this is the simplified pipeline, using a CompositeTransform for Task 2
def run_simple_pipeline():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Create file lines' >> beam.io.ReadFromText(input_file_name, skip_header_lines=1)
            | 'Create a dict from the CSV' >> CalculateDailyTransactions()
            | 'Write to file and compress' >> beam.io.WriteToText(
            output_file_name,
            file_name_suffix='.jsonl.gz',
            shard_name_template='',
            compression_type=CompressionTypes.GZIP)  # note that I could only get this result uncompressed using `gzip -dv <filename>` in the terminal
        )


# this is the pipeline without separating the core transformations to a standalone CompositeTransform
def run_pipeline():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Create file lines' >> beam.io.ReadFromText(input_file_name, skip_header_lines=1)
            | 'Create a dict from the CSV' >> beam.ParDo(DataRow())
            | 'Select transactions with values greater than 20' >>
            beam.Filter(lambda entry: entry.transaction_amount > 20)
            | 'Remove transactions from before 2010' >>
            beam.Filter(lambda entry: entry.timestamp > datetime.strptime("2010-01-01", "%Y-%m-%d").date())
            | 'Group transactions by date' >> beam.GroupBy(lambda s: str(s.timestamp)).aggregate_field(
            'transaction_amount', sum, 'total_amount')
            | 'Create dictionary' >> beam.ParDo(RowsToDict())
            | 'Format dictionary as CSV' >> beam.ParDo(DictToCSVFn(['date', 'total_amount']))
            | 'Write to file and compress' >> beam.io.WriteToText(
            output_file_name,
            file_name_suffix='.jsonl.gz',
            shard_name_template='',
            compression_type=CompressionTypes.GZIP)
        )


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_simple_pipeline()
