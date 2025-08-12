# مسیر فایل: client_agent.py (نسخه نهایی با قابلیت رمزگذاری)

import os
import requests
import socket
import platform
import configparser
from pathlib import Path
from datetime import datetime
import subprocess
import gzip
import shutil
import time
import argparse

# ایمپورت‌های جدید
from utils.security import generate_key, encrypt_file, decrypt_file
from drivers.postgres_driver import PostgresDriver
from drivers.mysql_driver import MySQLDriver


class ClientAgent:
    def __init__(self, server_url: str):
        self.server_url = server_url
        self.config_path = Path("client_config.ini")
        self.config = configparser.ConfigParser()
        self.access_token = None
        self.encryption_key = None
        self.paths_config = {}
        self.temp_dir = Path("./temp_backups")
        self.temp_dir.mkdir(exist_ok=True)
        self._load_config()

    def _load_config(self):
        if self.config_path.exists():
            self.config.read(self.config_path)
            if 'Auth' in self.config and 'AccessToken' in self.config['Auth']:
                self.access_token = self.config['Auth']['AccessToken']

            if 'Security' in self.config and 'EncryptionKey' in self.config['Security']:
                self.encryption_key = self.config['Security']['EncryptionKey'].encode()

            if 'Paths' in self.config:
                self.paths_config = dict(self.config['Paths'])

    def _save_config(self):
        with open(self.config_path, 'w') as f:
            self.config.write(f)

    def save_encryption_key(self, key: bytes):
        """کلید رمزگذاری را در فایل کانفیگ ذخیره می‌کند."""
        if 'Security' not in self.config:
            self.config['Security'] = {}
        self.config['Security']['EncryptionKey'] = key.decode()
        self._save_config()
        self.encryption_key = key
        print("✅ کلید رمزگذاری با موفقیت تولید و در 'client_config.ini' ذخیره شد.")

    def register(self):
        if self.access_token: return
        print("🚀 در حال ثبت‌نام کلاینت...")
        try:
            payload = {"hostname": socket.gethostname(), "os_type": platform.system().lower()}
            response = requests.post(f"{self.server_url}/api/v1/clients/register", json=payload, timeout=10)
            response.raise_for_status()
            self.access_token = response.json().get("access_token")
            if self.access_token:
                print("✅ ثبت‌نام موفق!")
                self._save_config()
        except Exception as e:
            raise RuntimeError(f"ثبت‌نام اولیه ناموفق بود: {e}")

    def get_headers(self) -> dict:
        if not self.access_token: raise ValueError("Token not found.")
        return {"Authorization": f"Bearer {self.access_token}"}

    def _get_driver(self, job_config):
        """بر اساس کانفیگ، درایور مناسب را به همراه مسیر ابزارها برمی‌گرداند."""
        db_type = job_config.get("type")

        # << به این بخش دقت کنید >>
        bin_path = None
        if db_type == "postgresql":
            bin_path = self.paths_config.get("postgres_bin_path")
            return PostgresDriver(job_config["config"], self.temp_dir, bin_path)
        elif db_type == "mysql":
            bin_path = self.paths_config.get("mysql_bin_path")
            return MySQLDriver(job_config["config"], self.temp_dir, bin_path)
        else:
            raise ValueError(f"درایور برای دیتابیس نوع '{db_type}' پشتیبانی نمی‌شود.")

    def upload_backup(self, file_path: Path, bucket_name: str):
        print(f"📤 [{datetime.now()}] در حال آپلود فایل '{file_path.name}'...")
        upload_url = f"{self.server_url}/api/v1/storage/upload/{bucket_name}/{file_path.name}"
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
            headers = self.get_headers()
            headers['Content-Type'] = 'application/octet-stream'
            response = requests.put(upload_url, data=data, headers=headers, timeout=300)
            response.raise_for_status()
            print(f"🎉 [{datetime.now()}] آپلود موفق!")
            return True
        except Exception as e:
            raise RuntimeError(f"آپلود ناموفق بود: {e}")

    def list_backups(self, bucket_name: str) -> list:
        print(f"🔍 در حال دریافت لیست بکاپ‌ها از باکت '{bucket_name}'...")
        list_url = f"{self.server_url}/api/v1/storage/list/{bucket_name}"
        try:
            response = requests.get(list_url, headers=self.get_headers(), timeout=60)
            response.raise_for_status()
            files = response.json().get("files", [])
            print(f"✅ {len(files)} فایل بکاپ یافت شد.")
            return files
        except Exception as e:
            raise RuntimeError(f"دریافت لیست بکاپ‌ها ناموفق بود: {e}")

    def run_backup_job(self, job_config: dict):
        db_name = job_config['config'].get('dbname') or job_config['config'].get('database')
        print(f"--- شروع چرخه امن پشتیبان‌گیری برای '{db_name}' ---")

        if not self.encryption_key:
            raise RuntimeError("کلید رمزگذاری یافت نشد. لطفاً ابتدا با دستور 'generate-key' یک کلید بسازید.")

        compressed_path = None
        encrypted_path = None
        try:
            # ۱. ایجاد بکاپ و فشرده‌سازی
            driver = self._get_driver(job_config)
            compressed_path = driver.backup()

            # ۲. رمزگذاری فایل فشرده
            encrypted_path = compressed_path.with_suffix(compressed_path.suffix + '.enc')
            print(f"🔒 در حال رمزگذاری فایل بکاپ...")
            encrypt_file(self.encryption_key, compressed_path, encrypted_path)
            print("✅ رمزگذاری با موفقیت انجام شد.")

            # ۳. آپلود فایل رمزگذاری شده
            if encrypted_path and encrypted_path.exists():
                self.upload_backup(encrypted_path, job_config["bucket"])

        except Exception as e:
            print(f"🔥 یک خطای کلی در چرخه پشتیبان‌گیری رخ داد: {e}")
        finally:
            # ۴. پاکسازی تمام فایل‌های موقت
            if compressed_path and compressed_path.exists(): compressed_path.unlink()
            if encrypted_path and encrypted_path.exists(): encrypted_path.unlink()
            print("🗑️ فایل‌های موقت پاک شدند.")
        print("--- پایان چرخه امن پشتیبان‌گیری ---")

    def run_restore_job(self, job_config: dict, object_name: str):
        if not object_name.endswith('.enc'):
            raise ValueError("فایل انتخابی یک فایل رمزگذاری شده (با پسوند .enc) نیست.")
        if not self.encryption_key:
            raise RuntimeError("کلید رمزگذاری یافت نشد. امکان رمزگشایی وجود ندارد.")

        db_name = job_config['config'].get('dbname') or job_config['config'].get('database')
        print(f"--- شروع چرخه امن بازیابی برای '{db_name}' ---")

        encrypted_path = self.temp_dir / object_name
        compressed_path = self.temp_dir / object_name.removesuffix('.enc')
        raw_path = self.temp_dir / compressed_path.name.removesuffix('.gz')

        try:
            # ۱. دانلود فایل رمزگذاری شده
            # (متد upload_backup از قبل فایل را با نام کامل آپلود می‌کند)
            self.download_backup(object_name, job_config['bucket'], encrypted_path)

            # ۲. رمزگشایی فایل
            print(f"🔑 در حال رمزگشایی فایل '{encrypted_path.name}'...")
            decrypt_file(self.encryption_key, encrypted_path, compressed_path)
            print("✅ رمزگشایی با موفقیت انجام شد.")

            # ۳. از حالت فشرده خارج کردن
            print(f"📦 در حال استخراج فایل '{compressed_path.name}'...")
            with gzip.open(compressed_path, 'rb') as f_in, open(raw_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            # ۴. اجرای بازیابی
            driver = self._get_driver(job_config)
            driver.restore(raw_path)

        except Exception as e:
            print(f"🔥 یک خطای کلی در چرخه بازیابی رخ داد: {e}")
        finally:
            if encrypted_path and encrypted_path.exists(): encrypted_path.unlink()
            if compressed_path and compressed_path.exists(): compressed_path.unlink()
            if raw_path and raw_path.exists(): raw_path.unlink()
            print("🗑️ تمام فایل‌های موقت بازیابی پاک شدند.")
        print("--- پایان چرخه امن بازیابی ---")

    # متد download_backup را به کلاس اضافه می‌کنیم
    def download_backup(self, object_name: str, bucket_name: str, destination_path: Path):
        print(f"📥 [{datetime.now()}] در حال دانلود فایل '{object_name}'...")
        download_url = f"{self.server_url}/api/v1/storage/download/{bucket_name}/{object_name}"
        try:
            response = requests.get(download_url, headers=self.get_headers(), timeout=300, stream=True)
            response.raise_for_status()
            with open(destination_path, 'wb') as f:
                shutil.copyfileobj(response.raw, f)
            print("✅ دانلود با موفقیت انجام شد.")
            return True
        except Exception as e:
            raise RuntimeError(f"دانلود ناموفق بود: {e}")


if __name__ == "__main__":
    # --- تعریف کانفیگ‌های مختلف برای دیتابیس‌ها ---
    # این بخش را با اطلاعات واقعی دیتابیس‌های خود پر کنید
    JOBS = {
        "pg_main": {
            "type": "postgresql",
            "bucket": "pg-main-backups",
            "config": {
                "host": "localhost", "port": 5432, "dbname": "online_shop",
                "user": "postgres", "password": "12345678"
            }
        },
        "mysql_web": {
            "type": "mysql",
            "bucket": "mysql-web-backups",
            "config": {
                "host": "localhost", "port": 3306, "database": "your_mysql_db",
                "user": "your_mysql_user", "password": "your_mysql_password"
            }
        }
    }

    # --- ساختار صحیح و جدید Argparse ---
    parser = argparse.ArgumentParser(
        description="کلاینت امن سیستم پشتیبان‌گیری",
        formatter_class=argparse.RawTextHelpFormatter  # برای نمایش بهتر help
    )
    subparsers = parser.add_subparsers(dest='action', required=True, help="عملیات مورد نظر")

    # کامند 1: generate-key (بدون نیاز به آرگومان اضافی)
    parser_keygen = subparsers.add_parser('generate-key', help="یک کلید رمزگذاری جدید تولید و ذخیره می‌کند")

    # کامند 2: backup
    parser_backup = subparsers.add_parser('backup', help="از دیتابیس مشخص شده یک بکاپ جدید ایجاد می‌کند")
    parser_backup.add_argument('--job', choices=JOBS.keys(), required=True, help="نام وظیفه‌ای که باید اجرا شود")

    # کامند 3: list
    parser_list = subparsers.add_parser('list', help="لیست بکاپ‌های موجود برای یک وظیفه را نمایش می‌دهد")
    parser_list.add_argument('--job', choices=JOBS.keys(), required=True,
                             help="نام وظیفه‌ای که لیست بکاپ‌های آن نمایش داده شود")

    # کامند 4: restore
    parser_restore = subparsers.add_parser('restore', help="یک بکاپ مشخص را بازیابی می‌کند")
    parser_restore.add_argument('--job', choices=JOBS.keys(), required=True,
                                help="نام وظیفه‌ای که بکاپ روی آن بازیابی می‌شود")
    parser_restore.add_argument('--file', required=True, help="نام کامل فایل بکاپ برای بازیابی (با پسوند .enc)")

    args = parser.parse_args()

    agent = ClientAgent(server_url="http://127.0.0.1:8000")

    try:
        if args.action == 'generate-key':
            key = generate_key()
            agent.save_encryption_key(key)
        else:
            # تمام عملیات دیگر نیازمند انتخاب یک job و ثبت بودن کلاینت هستند
            selected_job_config = JOBS[args.job]

            if not agent.access_token:
                agent.register()

            if args.action == 'backup':
                agent.run_backup_job(selected_job_config)

            elif args.action == 'list':
                backups = agent.list_backups(selected_job_config['bucket'])
                if backups:
                    print(f"\n--- لیست بکاپ‌های موجود در باکت '{selected_job_config['bucket']}' ---")
                    for f in backups:
                        print(f"  - {f}")
                    print("-------------------------------------------------")

            elif args.action == 'restore':
                agent.run_restore_job(selected_job_config, args.file)

    except (ValueError, RuntimeError, FileNotFoundError) as e:
        print(f"\n🔴 یک خطای عملیاتی رخ داد: {e}")
    except Exception as e:
        print(f"\n🔴 یک خطای پیش‌بینی نشده رخ داد: {e}")