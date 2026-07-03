import io
from typing import List
from minio import Minio

class MinIOStore:
    """MinIO client wrapper for Iceberg storage operations."""
    
    def __init__(self, endpoint: str = "127.0.0.1:9000", 
                 access_key: str = "admin", 
                 secret_key: str = "password", 
                 bucket_name: str = "iceberg", 
                 secure: bool = False):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
        
        # Ensure the bucket exists automatically
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def put(self, key: str, data: bytes) -> None:
        """Upload bytes to MinIO."""
        self.client.put_object(
            self.bucket_name,
            key,
            data=io.BytesIO(data),
            length=len(data)
        )

    def get(self, key: str) -> bytes:
        """Download bytes from MinIO."""
        response = None
        try:
            response = self.client.get_object(self.bucket_name, key)
            return response.read()
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def list(self, prefix: str) -> List[str]:
        """List keys in the bucket matching the given prefix."""
        # Using recursive=True to get all objects under a "directory" prefix
        objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
