import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToBigQuery
import logging

# Define a function to process the CSV line
class DataTransformation(beam.DoFn):
    def process(self, element):
        try:
            # Parse the CSV line
            columns = element.split(',')
            # Create a dictionary with processed data
            data = {
                'firstname': columns[0].strip(),
                'lastname': columns[1].strip(),
                'age': int(columns[2].strip()),  # Ensure age is an integer
                'gender': columns[3].strip()
            }
            return [data]
        except Exception as e:
            logging.error(f"Error processing element: {element}, Error: {str(e)}")
            return []

def run_pipeline(argv=None):
    # Create an argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='GCS file path for input CSV file')
    parser.add_argument('--output', dest='output', required=True, help='BigQuery table in the format project:dataset.table')
    
    # Parse the command-line arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create PipelineOptions using the parsed arguments
    pipeline_options = PipelineOptions(pipeline_args)

    data_trans = DataTransformation()

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read data from the GCS CSV file
        lines = p | 'ReadFromGCS' >> ReadFromText(known_args.input)

        # Process each CSV line
        final_data = lines | 'ProcessData' >> beam.ParDo(data_trans)

        # Write the processed data to BigQuery
        final_data | 'WriteToBigQuery' >> WriteToBigQuery(
            table=known_args.output,
            schema='firstname:STRING, lastname:STRING, age:INTEGER, gender:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
