# app/encryption.py
from typing import Optional
import os
from cryptography.fernet import Fernet, InvalidToken

# Read the key from env var; if not set, encryption is disabled
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")
_f = None
if ENCRYPTION_KEY:
    try:
        _f = Fernet(ENCRYPTION_KEY.encode() if isinstance(ENCRYPTION_KEY, str) else ENCRYPTION_KEY)
    except Exception:
        _f = None

def is_enabled() -> bool:
    return _f is not None

def encrypt_bytes(data: bytes) -> bytes:
    """Return encrypted bytes when encryption is enabled, else original bytes."""
    if _f:
        return _f.encrypt(data)
    return data

def decrypt_bytes(data: bytes) -> bytes:
    """Attempt decrypt if enabled; if decryption fails or not enabled, return original."""
    if _f:
        try:
            return _f.decrypt(data)
        except InvalidToken:
            # if key doesn't match, surface original bytes (caller must handle)
            return data
    return data
