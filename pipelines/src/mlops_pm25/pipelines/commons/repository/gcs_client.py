from google.cloud import storage
from datetime import timedelta
from mlops_pm25.pipelines.commons.domain.constants import DEFAULT_EXPIRATION_TIME

class GcsClient:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        pass

    def generate_signed_url(self, blob_name, expiration_minutes=DEFAULT_EXPIRATION_TIME):
        blob = self.bucket.blob(blob_name)

        url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=expiration_minutes),
            method="GET",
        )

        return url