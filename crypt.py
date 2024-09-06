"""
Simple Encrypt/Decrypt Data With Password
Martijn Pietres (stackoverflow.com)
edit by tohid.jk
2023-03-22
"""


import secrets
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


iterations = 1024


def derive_key(password: bytes, salt: bytes, iterations: int = iterations) -> bytes:
    """Derive a secret key from a given password and salt"""

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=iterations,
        backend=default_backend()
    )
    return b64e(kdf.derive(password))

def encrypt(message: bytes, password: str, iterations: int = iterations) -> bytes:
    salt = secrets.token_bytes(16)
    key = derive_key(password.encode(), salt, iterations)
    return b64e(
        b"%b%b%b" % (
            salt,
            iterations.to_bytes(4, "big"),
            b64d(Fernet(key).encrypt(message)),
        )
    )

def decrypt(token: bytes, password: str) -> bytes:
    decoded = b64d(token)
    salt, iter, token = decoded[:16], decoded[16:20], b64e(decoded[20:])
    iterations = int.from_bytes(iter, "big")
    key = derive_key(password.encode(), salt, iterations)
    return Fernet(key).decrypt(token)


if __name__ == "__main__":
    message = "John Doe"
    print(message)
    password = "mypass"
    print(password)
    token = encrypt(message.encode(), password)
    print(token)
    decrypted = decrypt(token, password).decode()
    print(decrypted)

