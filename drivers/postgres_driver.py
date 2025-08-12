# مسیر فایل: drivers/postgres_driver.py
import platform
import subprocess
import gzip
import shutil
from datetime import datetime
from pathlib import Path
import os
from .base_driver import BaseDriver

import subprocess
import gzip
import shutil
from datetime import datetime
from pathlib import Path
import os
from .base_driver import BaseDriver


class PostgresDriver(BaseDriver):
    def __init__(self, db_config: dict, temp_dir: Path, bin_path: str = None):
        """سازنده درایور، مسیر اختیاری پوشه bin را دریافت می‌کند."""
        super().__init__(db_config, temp_dir)
        self.bin_path = Path(bin_path) if bin_path else None
        self.tool_paths = {}  # برای کش کردن مسیر ابزارها

    def _get_tool_path(self, tool_name: str) -> str:
        """مسیر کامل یک ابزار را پیدا کرده و برای استفاده‌های بعدی کش می‌کند."""
        if tool_name in self.tool_paths:
            return self.tool_paths[tool_name]

        # در ویندوز، ابزارها پسوند .exe دارند
        if platform.system() == "Windows":
            tool_name += ".exe"

        # اولویت با مسیری است که در کانفیگ مشخص شده
        if self.bin_path and (self.bin_path / tool_name).is_file():
            path = str(self.bin_path / tool_name)
            self.tool_paths[tool_name] = path
            return path

        # اگر در کانفیگ نبود، در PATH سیستم جستجو کن
        path_from_which = shutil.which(tool_name)
        if path_from_which:
            self.tool_paths[tool_name] = path_from_which
            return path_from_which

        # اگر هیچ‌کجا پیدا نشد، خطا بده
        raise FileNotFoundError(f"ابزار '{tool_name}' نه در مسیر مشخص شده و نه در PATH سیستم یافت نشد.")

    def backup(self) -> Path:
        db_name = self.db_config['dbname']
        print(f"🚀 [{datetime.now()}] شروع پشتیبان‌گیری از PostgreSQL: {db_name}...")

        # مسیر کامل ابزار را دریافت کن
        pg_dump_path = self._get_tool_path("pg_dump")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_backup_path = self.temp_dir / f"{db_name}_{timestamp}.sql"
        compressed_backup_path = self.temp_dir / f"{db_name}_{timestamp}.sql.gz"

        env = {**os.environ, "PGPASSWORD": self.db_config.get("password", "")}

        # این لیست command صحیح است
        command = [
            pg_dump_path,
            "-h", self.db_config.get("host"),
            "-p", str(self.db_config.get("port")),
            "-U", self.db_config.get("user"),
            "-d", db_name,
            "-f", str(raw_backup_path)
        ]

        try:
            subprocess.run(command, check=True, capture_output=True, text=True, env=env)
            with open(raw_backup_path, 'rb') as f_in, gzip.open(compressed_backup_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            return compressed_backup_path
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"پشتیبان‌گیری PostgreSQL شکست خورد: {e.stderr}") from e
        finally:
            if raw_backup_path.exists():
                raw_backup_path.unlink()

    def restore(self, backup_file_path: Path):
        db_name = self.db_config['dbname']
        print(f"🔄 [{datetime.now()}] شروع بازیابی PostgreSQL: {db_name}...")

        psql_path = self._get_tool_path("psql")
        dropdb_path = self._get_tool_path("dropdb")
        createdb_path = self._get_tool_path("createdb")

        print(f"🔄 [{datetime.now()}] شروع بازیابی PostgreSQL: {db_name}...")

        env = {**os.environ, "PGPASSWORD": self.db_config.get("password", "")}
        common_args = ["-h", self.db_config.get("host"), "-p", str(self.db_config.get("port")), "-U",
                       self.db_config.get("user")]

        try:
            print(f"⚠️ احتیاط: در حال حذف و ایجاد مجدد دیتابیس '{db_name}'...")
            subprocess.run([dropdb_path, *common_args, db_name], check=True, capture_output=True, env=env)
            subprocess.run([createdb_path, *common_args, db_name], check=True, capture_output=True, env=env)

            restore_command = [psql_path, *common_args, "-d", db_name, "-f", str(backup_file_path)]
            subprocess.run(restore_command, check=True, capture_output=True, text=True, env=env)
            print(f"✅ بازیابی دیتابیس '{db_name}' با موفقیت کامل شد.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"بازیابی PostgreSQL شکست خورد: {e.stderr}") from e