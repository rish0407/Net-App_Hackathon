# app/storage_adapters.py
import os
import shutil
from pathlib import Path
from typing import Optional
import boto3
from botocore.exceptions import ClientError

from app.encryption import encrypt_bytes, decrypt_bytes, is_enabled

# Root folders for simulated stores (set via env or use defaults)
ONPREM_ROOT = Path(os.getenv("ONPREM_ROOT", "/tmp/data_in_motion/onprem"))
PRIVATE_ROOT = Path(os.getenv("PRIVATE_ROOT", "/tmp/data_in_motion/private"))
PUBLIC_ROOT = Path(os.getenv("PUBLIC_ROOT", "/tmp/data_in_motion/public"))

# Ensure directories exist
for p in (ONPREM_ROOT, PRIVATE_ROOT, PUBLIC_ROOT):
    p.mkdir(parents=True, exist_ok=True)

def _ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

class LocalAdapter:
    """Simple adapter storing files under a configured root folder. Supports optional encryption."""
    def __init__(self, root: Path):
        self.root = Path(root)

    def uri_for(self, dataset_id: int) -> str:
        p = self.root / f"dataset_{dataset_id}.dat"
        return f"file://{p}"

    def exists(self, uri: str) -> bool:
        p = Path(uri.replace("file://", ""))
        return p.exists()

    def upload_placeholder(self, dataset_id:int, metadata:Optional[dict]=None):
        """Create a tiny placeholder file representing the dataset object."""
        p = self.root / f"dataset_{dataset_id}.dat"
        _ensure_parent(p)
        content = f"dataset:{dataset_id}\n".encode()
        if metadata:
            content += (str(metadata) + "\n").encode()

        # encrypt bytes if enabled
        content_to_write = encrypt_bytes(content)
        with open(p, "wb") as f:
            f.write(content_to_write)
        return f"file://{p}"

    def copy_from(self, src_uri: Optional[str], dataset_id:int):
        """Copy from src_uri into this adapter root. If src_uri is None, create placeholder."""
        dest = self.root / f"dataset_{dataset_id}.dat"
        _ensure_parent(dest)
        if not src_uri:
            return self.upload_placeholder(dataset_id)
        src_path = Path(src_uri.replace("file://", ""))
        if src_path.exists():
            # read source raw bytes, decrypt if necessary, then re-encrypt for destination
            with open(src_path, "rb") as fr:
                raw = fr.read()
            # try to decrypt (if source was encrypted)
            try:
                raw_plain = decrypt_bytes(raw)
            except Exception:
                raw_plain = raw
            # now encrypt for destination
            write_bytes = encrypt_bytes(raw_plain)
            with open(dest, "wb") as fw:
                fw.write(write_bytes)
            return f"file://{dest}"
        # If src doesn't exist (maybe remote), create placeholder
        return self.upload_placeholder(dataset_id, metadata={"copied_from": src_uri})

# S3 Adapter (real cloud)
class S3Adapter:
    """
    Minimal S3 adapter using boto3.
    Supports uploading a local file (or raw bytes) and generating a s3://bucket/key URI.
    """
    def __init__(self, bucket: str, prefix: str = ""):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        # allow custom endpoint (for MinIO)
        self.endpoint = os.getenv("S3_ENDPOINT", "")
        session = boto3.session.Session()
        self.s3 = session.client(
            "s3",
            endpoint_url=self.endpoint or None,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

    def _key_for(self, dataset_id:int) -> str:
        base = f"dataset_{dataset_id}.dat"
        return f"{self.prefix}/{base}" if self.prefix else base

    def uri_for(self, dataset_id:int) -> str:
        return f"s3://{self.bucket}/{self._key_for(dataset_id)}"

    def exists(self, uri:str) -> bool:
        key = self._key_for(int(uri.split("/")[-1].replace("dataset_","").replace(".dat","")))
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def upload_placeholder(self, dataset_id:int, metadata:Optional[dict]=None):
        key = self._key_for(dataset_id)
        content = f"dataset:{dataset_id}\n".encode()
        if metadata:
            content += (str(metadata) + "\n").encode()
        # encrypt bytes if needed
        content_to_write = encrypt_bytes(content)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=content_to_write)
        return f"s3://{self.bucket}/{key}"

    def copy_from(self, src_uri: Optional[str], dataset_id:int):
        """
        Copy content into S3. If src_uri is local file (file://) read & upload.
        If src_uri is s3://..., try to copy using S3 server-side copy.
        """
        key = self._key_for(dataset_id)
        if not src_uri:
            return self.upload_placeholder(dataset_id)
        if src_uri.startswith("file://"):
            src_path = Path(src_uri.replace("file://", ""))
            if src_path.exists():
                with open(src_path, "rb") as fr:
                    raw = fr.read()
                # decrypt if needed, then encrypt for S3 storage
                raw_plain = decrypt_bytes(raw)
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=encrypt_bytes(raw_plain))
                return f"s3://{self.bucket}/{key}"
        elif src_uri.startswith("s3://"):
            # attempt server-side copy (from another s3 uri)
            parts = src_uri.replace("s3://", "").split("/",1)
            src_bucket = parts[0]; src_key = parts[1] if len(parts)>1 else ""
            try:
                copy_source = {"Bucket": src_bucket, "Key": src_key}
                self.s3.copy(copy_source, self.bucket, key)
                return f"s3://{self.bucket}/{key}"
            except Exception:
                # fallback: download then upload (not implemented here)
                pass
        # fallback
        return self.upload_placeholder(dataset_id, metadata={"copied_from": src_uri})

# Instantiate adapters
onprem_adapter = LocalAdapter(ONPREM_ROOT)
private_adapter = LocalAdapter(PRIVATE_ROOT)
public_adapter = LocalAdapter(PUBLIC_ROOT)

# Optional: create an S3 adapter if env var set
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()
s3_adapter = S3Adapter(S3_BUCKET, S3_PREFIX) if S3_BUCKET else None

# convenience map
ADAPTER_MAP = {
    "ONPREM": onprem_adapter,
    "PRIVATE": private_adapter,
    "PUBLIC": s3_adapter if s3_adapter is not None else public_adapter,
}

def adapter_for_storage(storage_type: str):
    return ADAPTER_MAP.get(storage_type.upper(), onprem_adapter)
