from cryptography.fernet import Fernet
import os


def generate_key() -> bytes:
    """یک کلید امن جدید برای رمزگذاری تولید می‌کند."""
    return Fernet.generate_key()


def encrypt_file(key: bytes, input_file_path: str, output_file_path: str):
    """یک فایل را با استفاده از کلید داده شده رمزگذاری می‌کند."""
    fernet = Fernet(key)
    with open(input_file_path, 'rb') as file:
        original_data = file.read()

    encrypted_data = fernet.encrypt(original_data)

    with open(output_file_path, 'wb') as file:
        file.write(encrypted_data)


def decrypt_file(key: bytes, input_file_path: str, output_file_path: str):
    """یک فایل رمزگذاری شده را با استفاده از کلید داده شده رمزگشایی می‌کند."""
    fernet = Fernet(key)
    with open(input_file_path, 'rb') as file:
        encrypted_data = file.read()

    decrypted_data = fernet.decrypt(encrypted_data)

    with open(output_file_path, 'wb') as file:
        file.write(decrypted_data)