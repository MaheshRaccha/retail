import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import json

# Define the schema for BigQuery table
SCHEMA = 'CustomerID:INTEGER,Store:STRING,Timestamp:TIMESTAMP,TransactionID:INTEGER,EventType:STRING,ProductID:INTEGER,Amount:FLOAT,PaymentMethod:STRING,ProductCategory:STRING'

class ParseJson(beam.DoFn):
    def process(self, element):
        row = json.loads(element)
        yield {
            'CustomerID': int(row['CustomerID']),
            'Store': row['Store'],
            'Timestamp': row['Timestamp'],
            'TransactionID': int(row['TransactionID']),
            'EventType': row['EventType'],
            'ProductID': int(row['ProductID']),
            'Amount': float(row['Amount']),
            'PaymentMethod': row['PaymentMethod'],
            'ProductCategory': row['ProductCategory']
        }

def run(argv=None):
    # Define pipeline options
    pipeline_options = PipelineOptions()

    # Setting options for running on Dataflow
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'mahesh-raccha-gcp'
    google_cloud_options.job_name = 'gcs-to-bigquery-job'
    google_cloud_options.staging_location = 'gs://retail_storage/staging'
    google_cloud_options.temp_location = 'gs://retail_storage/temp'
    
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Path to the input file on GCS
    input_path = 'gs://retail_storage/snowflake/*.parquet'

    # BigQuery output table
    output_table = 'mahesh-raccha-gcp:retail_transactions.retail_pubsub'

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromGCS' >> beam.io.ReadFromText(input_path)
            | 'ParseJson' >> beam.ParDo(ParseJson())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                output_table,
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

if __name__ == '__main__':
    run()
