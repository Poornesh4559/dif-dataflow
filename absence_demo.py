import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import logging
import argparse
def process(element):
        # Split the row by the delimiter '||' and strip any whitespace
        colm = element.split('||')
        
        # Create a dictionary to map each value to its header
        if len(colm)==8:
            data = {
            'First_Day_of_Leave' : colm[0], 'Leave_Type_excluding_Family' : colm[1], 'Last_Day_of_Leave_Estimated' : colm[2], 'Last_Day_of_Leave_Actual' : colm[3], 'Leave_Reason' : colm[4], 'Worker_WID' : colm[5], 'Leave_of_Absence_Request_WID' : colm[6], 'Transaction_Status' :colm[7]
            }
            return data

# Define the pipeline options
options = PipelineOptions()

# Define the BigQuery schema
table_schema = {
    'fields': [
        {'name': 'First_Day_of_Leave', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Leave_Type_excluding_Family', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Last_Day_of_Leave_Estimated', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Last_Day_of_Leave_Actual', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Leave_Reason', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Worker_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Leave_of_Absence_Request_WID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Transaction_Status', 'type': 'STRING', 'mode': 'NULLABLE'}
        
    ]
}

# Set up the Beam pipeline


def run_pipeline(argv=None):
    # Create an argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',default='gs://half_pocdataflow/input/absancesample.csv',dest='input', required=False  , help='GCS file path for input CSV file')
    parser.add_argument('--output',default='half-poc:dif.absencesample',dest='output', required=False , help='BigQuery table in the format dataset.table')
    
    # Parse the command-line arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create PipelineOptions using the parsed arguments
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Create the pipeline
    with beam.Pipeline(options=options) as p:
        (
        p
        | 'Read CSV' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        | 'Process Rows' >> beam.Map(process)
        | 'Filter out null values' >> beam.Filter(lambda row: row is not None)
        | 'Write to BigQuery' >> WriteToBigQuery(
            table=known_args.output,
            schema = table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
