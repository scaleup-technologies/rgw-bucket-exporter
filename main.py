import os
import logging
from prometheus_client import Gauge, generate_latest
from requests_aws4auth import AWS4Auth
import requests
from flask import Flask, Response
import math
from urllib.parse import urlparse

class RGWBucketExporter:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)

        aws_access_key = os.getenv('S3_ACCESS_KEY')
        aws_secret_key = os.getenv('S3_SECRET_KEY')
        region = os.getenv('S3_REGION', 'us-east-1')
        self.admin_url = os.getenv('ADMIN_URL')
        self.verify = os.getenv('VERIFY_SSL', 'False').lower() == 'true'
        self.round_gbs = os.getenv('ROUND_GBS', 'True').lower() == 'true'

        self.auth = AWS4Auth(aws_access_key, aws_secret_key, region, 's3')

        self.bucket_usage_metric = Gauge('rgw_bucket_size_gb', 'RGW Bucket Size', ['bucket', 'tenant'])

        logging.info("RGWBucketExporter initialized with admin_url: %s", self.admin_url)

    def fetch_bucket_data(self):
        try:
            resp = requests.get(
                url=f"{self.admin_url}/bucket?stats=true&format=json",
                auth=self.auth,
                verify=self.verify,
                headers={'Host': urlparse(self.admin_url).hostname},
            )
            resp.raise_for_status()
            logging.info("Successfully fetched bucket data from: %s", self.admin_url)
            return resp.json()
        except requests.exceptions.RequestException as e:
            logging.error("Failed to fetch bucket data: %s", e)
            return []

    def update_metrics(self, data):
        for item in data:
            try:
                bucket = item['bucket']
                tenant = item['tenant']
                size_kb_utilized = item['usage']['rgw.main']['size_kb_utilized']
                size_gb_utilized = size_kb_utilized * 1024 / 1000000000
                if self.round_gbs:
                    size_gb_utilized = math.ceil(size_gb_utilized)
                self.bucket_usage_metric.labels(bucket, tenant).set(size_gb_utilized)
                logging.info("Updated metrics for bucket: %s, tenant: %s, size_kb_utilized: %s", bucket, tenant, size_kb_utilized)
            except KeyError as e:
                logging.warning("Missing expected data in response: %s", e)

    def collect_metrics(self):
        data = self.fetch_bucket_data()
        if data:
            self.update_metrics(data)
        else:
            logging.warning("No data to process.")

rgw_bucket_exporter = RGWBucketExporter()

app = Flask(__name__)

@app.route('/metrics')
def metrics():
    rgw_bucket_exporter.collect_metrics()
    return Response(generate_latest(), mimetype='text/plain')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9142)
    logging.info("Started HTTP server on port 9142")

