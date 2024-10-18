import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io import WriteToText


class ParseMessageFn(beam.DoFn):
    """Parses the Pub/Sub message data into a simple string."""
    def process(self, element):
        # Decode the message and yield the string content
        yield element.decode('utf-8')


def run():
    # Set up the pipeline options
    options = PipelineOptions()

    # Specify that we're running the pipeline on Dataflow
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Set Dataflow-specific options
    dataflow_options = options.view_as(PipelineOptions)
    dataflow_options.project = 'mahesh-raccha-gcp'  # Change to your GCP project ID
    dataflow_options.region = 'us-east1'  # Change to your GCP region
    dataflow_options.staging_location = 'gs://retail_storage/staging'  # Change to your GCS staging bucket
    dataflow_options.temp_location = 'gs://retail_storage/temp'  # Change to your GCS temp bucket
    dataflow_options.job_name = 'pubsub-to-gcs'

    # Define the Pub/Sub topic to read from
    input_topic = 'projects/mahesh-raccha-gcp/topics/retail_transactions'  # Replace with your Pub/Sub topic

    # Define the GCS output path
    output_path = 'gs://retail_storage/retail_transactions/pubsub-output'  # Replace with your GCS bucket path

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromPubSub' >> ReadFromPubSub(topic=input_topic)
            | 'ParseMessage' >> beam.ParDo(ParseMessageFn())
            | 'WriteToGCS' >> WriteToText(output_path)
        )


if __name__ == '__main__':
    run()
