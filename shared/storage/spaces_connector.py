from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Optional, Iterable

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class SpacesConfig:
    bucket: str
    region: str
    access_key: str
    secret_key: str
    endpoint_url: str

    @staticmethod
    def from_env(prefix: str = "DO_SPACES_") -> "SpacesConfig":
        bucket = os.environ[f"{prefix}BUCKET"]
        region = os.environ[f"{prefix}REGION"]
        access_key = os.environ[f"{prefix}KEY"]
        secret_key = os.environ[f"{prefix}SECRET"]
        endpoint_url = os.environ.get(f"{prefix}ENDPOINT")
        return SpacesConfig(
            bucket=bucket,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
        )


class SpacesConnector:
    def __init__(self, cfg: SpacesConfig):
        self.cfg = cfg
        self.client = boto3.client(
            "s3",
            region_name=cfg.region,
            endpoint_url=cfg.endpoint_url,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            config=BotoConfig(
                signature_version="s3v4",
                retries={"max_attempts": 8, "mode": "standard"},
            ),
        )

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.cfg.bucket, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def head(self, key: str) -> Optional[dict]:
        try:
            return self.client.head_object(Bucket=self.cfg.bucket, Key=key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def get_bytes(self, key: str, byte_range: Optional[tuple[int, int]] = None) -> bytes:
        params = {"Bucket": self.cfg.bucket, "Key": key}
        if byte_range is not None:
            start, end = byte_range
            params["Range"] = f"bytes={start}-{end}"
        resp = self.client.get_object(**params)
        body = resp["Body"].read()
        return body

    def get_json(self, key: str, encoding: str = "utf-8") -> Any:
        raw = self.get_bytes(key)
        return json.loads(raw.decode(encoding))

    def put_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        content_encoding: Optional[str] = None,
        cache_control: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> dict:
        extra: dict[str, Any] = {
            "Bucket": self.cfg.bucket,
            "Key": key,
            "Body": data,
            "ContentType": content_type,
        }
        if content_encoding:
            extra["ContentEncoding"] = content_encoding
        if cache_control:
            extra["CacheControl"] = cache_control
        if metadata:
            extra["Metadata"] = metadata
        return self.client.put_object(**extra)

    def put_json(
        self,
        key: str,
        obj: Any,
        encoding: str = "utf-8",
        cache_control: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> dict:
        data = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode(encoding)
        return self.put_bytes(
            key=key,
            data=data,
            content_type="application/json",
            cache_control=cache_control,
            metadata=metadata,
        )

    def upload_file(
        self,
        local_path: str,
        key: str,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        cache_control: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> None:
        extra_args: dict[str, Any] = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if content_encoding:
            extra_args["ContentEncoding"] = content_encoding
        if cache_control:
            extra_args["CacheControl"] = cache_control
        if metadata:
            extra_args["Metadata"] = metadata

        if extra_args:
            self.client.upload_file(local_path, self.cfg.bucket, key, ExtraArgs=extra_args)
        else:
            self.client.upload_file(local_path, self.cfg.bucket, key)

    def download_file(self, key: str, local_path: str) -> None:
        self.client.download_file(self.cfg.bucket, key, local_path)

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.cfg.bucket, Key=key)

    def list_keys(self, prefix: str, limit: int = 1000) -> list[str]:
        out: list[str] = []
        token: Optional[str] = None

        while True:
            kwargs: dict[str, Any] = {
                "Bucket": self.cfg.bucket,
                "Prefix": prefix,
                "MaxKeys": min(limit - len(out), 1000),
            }
            if token:
                kwargs["ContinuationToken"] = token

            resp = self.client.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                out.append(item["Key"])
                if len(out) >= limit:
                    return out

            if not resp.get("IsTruncated"):
                return out
            token = resp.get("NextContinuationToken")

    def presigned_get_url(self, key: str, expires_seconds: int = 900) -> str:
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.cfg.bucket, "Key": key},
            ExpiresIn=expires_seconds,
        )
