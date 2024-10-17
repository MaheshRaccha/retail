import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.gcsio import WriteToText
import json

def run():
    # Define your pipeline options
    options = PipelineOptions(
        project='mahesh-raccha-gcp',  # Replace with your GCP project ID
        temp_location='gs://retail_storage/temp',  # Replace with your GCS temp bucket
        staging_location='gs://retail_storage/staging',  # Replace with your GCS staging bucket
        runner='DataflowRunner',
        region='us-east1'  # Region where the pipeline will run
    )
    
    # Pub/Sub subscription path
    subscription = 'projects/mahesh-raccha-gcp/subscriptions/retail_transactions-sub'  # Replace with your subscription
    
    # Output path in GCS
    output_path = 'gs://mahesh-raccha-gcp/retail_transactions/output'  # Replace with your GCS bucket and path
    
    # Define the pipeline
    p = beam.Pipeline(options=options)
    
    # Read from Pub/Sub
    messages = (p
                | 'Read from PubSub' >> ReadFromPubSub(subscription=subscription)
                | 'Decode Messages' >> beam.Map(lambda msg: msg.decode('utf-8'))
                | 'Parse JSON' >> beam.Map(lambda msg: json.loads(msg))
                )
    
    # Write to GCS in JSON format
    (messages
     | 'Write to GCS' >> beam.io.WriteToText(output_path, file_name_suffix='.json', shard_name_template=''))

    # Run the pipeline
    p.run().wait_until_finish()

if __name__ == "__main__":
    run()
