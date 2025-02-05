import hashlib
from typing import Optional

from loguru import logger


def hash160(data: bytes) -> bytes:
    """Perform SHA256 followed by RIPEMD160 on the data."""
    sha = hashlib.sha256(data).digest()
    ripemd = hashlib.new("ripemd160", sha).digest()
    return ripemd


def base58_check_encode(payload: bytes) -> str:
    """Encode payload using Base58Check encoding."""
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    full_payload = payload + checksum
    num = int.from_bytes(full_payload, "big")
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    encoded = ""
    while num > 0:
        num, mod = divmod(num, 58)
        encoded = alphabet[mod] + encoded
    # Add '1' for each leading 0 byte in payload.
    n_leading = len(full_payload) - len(full_payload.lstrip(b"\x00"))
    return "1" * n_leading + encoded


def derive_address_from_pubkey(script_hex: str) -> Optional[str]:
    """
    Derives a P2PKH address from a P2PK script hex.
    Expects the script to be in the format:
      - Uncompressed: "41{65-byte pubkey}ac"
    Returns the Base58Check encoded address (using version byte 0x00) or None on error.
    """
    try:
        script_bytes = bytes.fromhex(script_hex)
        if script_bytes[-1] != 0xAC:
            return None
        if script_bytes[0] != 0x41 or len(script_bytes) != 67:
            return None
        pubkey = script_bytes[1:-1]
        h160 = hash160(pubkey)
        payload = b"\x00" + h160
        return base58_check_encode(payload)
    except Exception as e:
        logger.error(f"Error deriving address from pubkey: {e}")
        return None
