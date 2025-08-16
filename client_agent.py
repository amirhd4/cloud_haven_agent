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
import asyncio
import websockets
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# فرض بر این است که این فایل‌ها در کنار agent وجود دارند
from utils.security import generate_key, encrypt_file, decrypt_file
from drivers.postgres_driver import PostgresDriver
from drivers.mysql_driver import MySQLDriver

# ==============================================================================
#  بخش کانفیگ جاب‌ها - منبع حقیقت برای اطلاعات اتصال اکنون اینجاست
# ==============================================================================
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


# ==============================================================================

class ClientAgent:
    def __init__(self, server_url: str, jobs_config: dict):
        self.server_url = server_url
        self.jobs_config = jobs_config
        self.config_path = Path("client_config.ini")
        self.config = configparser.ConfigParser()
        self.access_token = None
        self.encryption_key = None
        self.paths_config = {}
        self.temp_dir = Path("./temp_backups")
        self.temp_dir.mkdir(exist_ok=True)
        self._load_config()
        self.no_proxy = {
            "http": None,
            "https": None,
        }

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
        if 'Security' not in self.config: self.config['Security'] = {}
        self.config['Security']['EncryptionKey'] = key.decode()
        self._save_config()
        self.encryption_key = key
        print("✅ کلید رمزگذاری با موفقیت تولید و در 'client_config.ini' ذخیره شد.")

    def get_headers(self) -> dict:
        if not self.access_token: raise ValueError("Token not found.")
        return {"Authorization": f"Bearer {self.access_token}"}

    def _get_driver(self, job_config):
        db_type = job_config.get("type")
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
            response = requests.put(upload_url, data=data, headers=headers, timeout=300, proxies=self.no_proxy)
            response.raise_for_status()
            print(f"🎉 [{datetime.now()}] آپلود موفق!")
            return True
        except Exception as e:
            raise RuntimeError(f"آپلود ناموفق بود: {e}")

    def download_backup(self, object_name: str, bucket_name: str, destination_path: Path):
        print(f"📥 [{datetime.now()}] در حال دانلود فایل '{object_name}'...")
        download_url = f"{self.server_url}/api/v1/storage/download/{bucket_name}/{object_name}"
        try:
            response = requests.get(download_url, headers=self.get_headers(), timeout=300, stream=True, proxies=self.no_proxy)
            response.raise_for_status()
            with open(destination_path, 'wb') as f:
                shutil.copyfileobj(response.raw, f)
            print("✅ دانلود با موفقیت انجام شد.")
            return True
        except Exception as e:
            raise RuntimeError(f"دانلود ناموفق بود: {e}")

    def list_backups(self, bucket_name: str) -> list:
        print(f"🔍 در حال دریافت لیست بکاپ‌ها از باکت '{bucket_name}'...")
        list_url = f"{self.server_url}/api/v1/storage/list/{bucket_name}"
        try:
            response = requests.get(list_url, headers=self.get_headers(), timeout=60, proxies=self.no_proxy)
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
        compressed_path, encrypted_path = None, None
        try:
            driver = self._get_driver(job_config)
            compressed_path = driver.backup()
            encrypted_path = compressed_path.with_suffix(compressed_path.suffix + '.enc')
            print(f"🔒 در حال رمزگذاری فایل بکاپ...")
            encrypt_file(self.encryption_key, compressed_path, encrypted_path)
            print("✅ رمزگذاری با موفقیت انجام شد.")
            if encrypted_path and encrypted_path.exists():
                self.upload_backup(encrypted_path, job_config["bucket"])
        except Exception as e:
            print(f"🔥 یک خطای کلی در چرخه پشتیبان‌گیری رخ داد: {e}")
        finally:
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
            self.download_backup(object_name, job_config['bucket'], encrypted_path)
            print(f"🔑 در حال رمزگشایی فایل '{encrypted_path.name}'...")
            decrypt_file(self.encryption_key, encrypted_path, compressed_path)
            print("✅ رمزگشایی با موفقیت انجام شد.")
            print(f"📦 در حال استخراج فایل '{compressed_path.name}'...")
            with gzip.open(compressed_path, 'rb') as f_in, open(raw_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            driver = self._get_driver(job_config)
            driver.restore(raw_path)
        except Exception as e:
            print(f"🔥 یک خطای کلی در چرخه بازیابی رخ داد: {e}")
        finally:
            if encrypted_path.exists(): encrypted_path.unlink()
            if compressed_path.exists(): compressed_path.unlink()
            if raw_path.exists(): raw_path.unlink()
            print("🗑️ تمام فایل‌های موقت بازیابی پاک شدند.")
        print("--- پایان چرخه امن بازیابی ---")

    async def _fetch_and_apply_schedules(self, scheduler: AsyncIOScheduler):
        """زمان‌بندی‌ها را از API اختصاصی Agent دریافت و در زمان‌بند محلی اعمال می‌کند."""
        print(f"[{datetime.now()}] در حال دریافت و به‌روزرسانی زمان‌بندی‌ها از سرور...")
        try:
            response = requests.get(f"{self.server_url}/api/v1/agent/my-schedules", headers=self.get_headers(), proxies=self.no_proxy)
            response.raise_for_status()
            schedules = response.json()

            scheduler.remove_all_jobs()
            print(f"[{datetime.now()}] به‌روزرسانی زمان‌بندی‌ها...")
            for schedule_data in schedules:
                if not schedule_data['is_active']: continue
                job_name = schedule_data['job_name']
                if job_name in self.jobs_config:
                    job_config = self.jobs_config[job_name]
                    print(f"  + زمان‌بندی وظیفه '{job_name}' با cron: '{schedule_data['cron_string']}'")
                    scheduler.add_job(
                        self.run_backup_job, 'cron',
                        **self._parse_cron(schedule_data['cron_string']),
                        kwargs={"job_config": job_config}
                    )
                else:
                    print(f"  - هشدار: جاب با نام '{job_name}' در کانفیگ محلی (JOBS) یافت نشد.")
        except Exception as e:
            print(f"❌ خطا در دریافت زمان‌بندی‌ها: {e}")

    @staticmethod
    def _parse_cron(cron_string: str) -> dict:
        parts = cron_string.split()
        if len(parts) != 5: return {}
        return {'minute': parts[0], 'hour': parts[1], 'day': parts[2], 'month': parts[3], 'day_of_week': parts[4]}

    async def _websocket_listener(self):
        scheduler = AsyncIOScheduler()
        await self._fetch_and_apply_schedules(scheduler)
        scheduler.start()
        print("\n✅ زمان‌بند محلی فعال شد.")

        ws_uri = f"ws://{self.server_url.split('//')[1]}/api/v1/clients/ws"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        while True:
            try:
                async with websockets.connect(ws_uri, additional_headers=headers, proxy=None) as websocket:
                    print("✅ با موفقیت به سرور WebSocket متصل شد.")

                    # --- معرفی خود با جزئیات کامل (نام جاب -> نام باکت) ---
                    jobs_map = {name: details['bucket'] for name, details in self.jobs_config.items()}
                    identify_payload = {
                        "type": "identify",
                        "jobs": jobs_map
                    }
                    await websocket.send(json.dumps(identify_payload))
                    print(f"ℹ️ قابلیت‌ها به سرور معرفی شد: {jobs_map}")
                    print("...منتظر دریافت فرمان...")

                    async for message in websocket:
                        print(f"\n📨 فرمان جدید دریافت شد: {message}")
                        command = json.loads(message)
                        action = command.get("action")

                        if action == "reload_schedules":
                            print("🔄 دریافت فرمان به‌روزرسانی زمان‌بندی...")
                            await self._fetch_and_apply_schedules(scheduler)
                            continue

                        job_name = command.get("job")
                        if not job_name or job_name not in self.jobs_config:
                            print(f"❌ فرمان نامعتبر: job '{job_name}' در کانفیگ محلی تعریف نشده است.")
                            continue

                        job_config = self.jobs_config[job_name]
                        if action == "backup":
                            self.run_backup_job(job_config)
                        elif action == "restore":
                            file_name = command.get("file")
                            if not file_name:
                                print("❌ فرمان نامعتبر: برای restore نام فایل الزامی است.")
                                continue
                            self.run_restore_job(job_config, file_name)
            except Exception as e:
                print(f"🔥 خطای WebSocket: {e}. تلاش برای اتصال مجدد تا 10 ثانیه دیگر...")
                await asyncio.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="کلاینت امن سیستم پشتیبان‌گیری")
    subparsers = parser.add_subparsers(dest='action', required=True)

    parser_keygen = subparsers.add_parser('generate-key', help="یک کلید رمزگذاری جدید تولید می‌کند.")
    parser_listen = subparsers.add_parser('listen', help="به عنوان یک سرویس اجرا شده و منتظر دستورات از سرور می‌ماند.")

    parser_backup = subparsers.add_parser('run-backup', help="یک وظیفه بکاپ را به صورت دستی اجرا می‌کند.")
    parser_backup.add_argument('--job', choices=JOBS.keys(), required=True, help="نام وظیفه‌ای که باید اجرا شود")

    parser_list = subparsers.add_parser('run-list', help="لیست بکاپ‌های یک جاب را به صورت دستی دریافت می‌کند.")
    parser_list.add_argument('--job', choices=JOBS.keys(), required=True)

    parser_restore = subparsers.add_parser('run-restore', help="یک بکاپ را به صورت دستی بازیابی می‌کند.")
    parser_restore.add_argument('--job', choices=JOBS.keys(), required=True)
    parser_restore.add_argument('--file', required=True)

    args = parser.parse_args()

    agent = ClientAgent(server_url="http://127.0.0.1:8000", jobs_config=JOBS)

    try:
        if args.action == 'generate-key':
            key = generate_key()
            agent.save_encryption_key(key)
        else:
            if not agent.access_token:
                raise ValueError(
                    "توکن دسترسی در client_config.ini یافت نشد. لطفاً ابتدا از طریق داشبورد ادمین، یک کلاینت بسازید و توکن آن را در این فایل قرار دهید.")

            if args.action == 'listen':
                if not agent.encryption_key: raise ValueError(
                    "کلید رمزگذاری در client_config.ini یافت نشد. لطفاً ابتدا دستور 'generate-key' را اجرا کنید.")
                asyncio.run(agent._websocket_listener())
            else:
                job_config = agent.jobs_config.get(args.job)
                # Note: job_config is guaranteed to exist due to 'choices=JOBS.keys()' in argparse

                if args.action == 'run-backup':
                    if not agent.encryption_key: raise ValueError("کلید رمزگذاری برای بکاپ الزامی است.")
                    agent.run_backup_job(job_config)
                elif args.action == 'run-list':
                    backups = agent.list_backups(job_config['bucket'])
                    if backups:
                        print(f"\n--- لیست بکاپ‌ها در باکت '{job_config['bucket']}' ---")
                        for f in backups: print(f"  - {f}")
                elif args.action == 'run-restore':
                    if not agent.encryption_key: raise ValueError("کلید رمزگذاری برای بازیابی الزامی است.")
                    agent.run_restore_job(job_config, args.file)

    except (ValueError, RuntimeError, FileNotFoundError) as e:
        print(f"\n🔴 یک خطای عملیاتی رخ داد: {e}")
    except Exception as e:
        print(f"\n🔴 یک خطای پیش‌بینی نشده رخ داد: {e}")