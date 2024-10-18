import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json

class ParseMessageFn(beam.DoFn):
    """Parses the Pub/Sub message data into a dictionary."""
    def process(self, element):
        message = json.loads(element.decode('utf-8'))
        return [message]

def run():
    # Set up the pipeline options
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    # Specify the GCP project and other settings
    google_cloud_options = options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
    google_cloud_options.project = 'mahesh-raccha-gcp'
    google_cloud_options.region = 'us-east1'
    google_cloud_options.staging_location = 'gs://retail_storage/staging'
    google_cloud_options.temp_location = 'gs://retail_storage/temp'
    google_cloud_options.job_name = 'pubsub-to-bigquery'

    # Define the Pub/Sub subscription to read from
    subscription = 'projects/mahesh-raccha-gcp/subscriptions/retail_transactions-sub'

    # Define the BigQuery table to write to
    table_spec = 'mahesh-raccha-gcp:your_dataset.your_table'

    # Define the schema explicitly (Example schema for the fields)
    schema = {
        'fields': [
            {'name': 'CustomerID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Store', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'TransactionID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'EventType', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ProductID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'PaymentMethod', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ProductCategory', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=subscription)
            | 'Parse Pub/Sub Message' >> beam.ParDo(ParseMessageFn())
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=table_spec,
                schema=schema,  # Explicitly define the schema here
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
