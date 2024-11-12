"""
Simple Encrypt/Decrypt Data With Password
Martijn Pietres (stackoverflow.com)
2024-11-12
"""

import secrets
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

ITERATIONS = 1024

def derive_key(password: bytes, salt: bytes, iterations: int = ITERATIONS) -> bytes:
    """Derive a secret key from a given password and salt"""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=iterations,
        backend=default_backend()
    )
    return b64e(kdf.derive(password))

def encrypt(data: bytes, password: bytes, iterations: int = ITERATIONS) -> bytes:
    salt = secrets.token_bytes(16)
    key = derive_key(password, salt, iterations)
    return b64e(
        b"%b%b%b" % (
            salt,
            iterations.to_bytes(4, "big"),
            b64d(Fernet(key).encrypt(data))
        )
    )

def decrypt(token: bytes, password: bytes) -> bytes:
    decoded = b64d(token)
    salt, iters, data = decoded[:16], decoded[16:20], b64e(decoded[20:])
    iterations = int.from_bytes(iters, "big")
    key = derive_key(password, salt, iterations)
    return Fernet(key).decrypt(data)

def test():
    msg, pwd = b"mymessage", b"mypass"
    print(f"msg: {msg}, pwd: {pwd}")
    token = encrypt(msg, pwd)
    print(f"token: {token}")
    decrypted = decrypt(token, pwd)
    print(f"decrypted: {decrypted}")

if __name__ == "__main__":
    test()

