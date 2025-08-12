import subprocess
import gzip
import shutil
from datetime import datetime
from pathlib import Path
import os
import platform

# ایمپورت کلاس پایه
from .base_driver import BaseDriver


class MySQLDriver(BaseDriver):
    """درایور مخصوص پشتیبان‌گیری و بازیابی دیتابیس MySQL."""

    def __init__(self, db_config: dict, temp_dir: Path, bin_path: str = None):
        """سازنده درایور، مسیر اختیاری پوشه bin را دریافت می‌کند."""
        super().__init__(db_config, temp_dir)
        self.bin_path = Path(bin_path) if bin_path else None
        self.tool_paths = {}

    def _get_tool_path(self, tool_name: str) -> str:
        """مسیر کامل یک ابزار را پیدا کرده و برای استفاده‌های بعدی کش می‌کند."""
        if tool_name in self.tool_paths:
            return self.tool_paths[tool_name]

        if platform.system() == "Windows":
            tool_name += ".exe"

        if self.bin_path and (self.bin_path / tool_name).is_file():
            path = str(self.bin_path / tool_name)
            self.tool_paths[tool_name] = path
            return path

        path_from_which = shutil.which(tool_name)
        if path_from_which:
            self.tool_paths[tool_name] = path_from_which
            return path_from_which

        raise FileNotFoundError(f"ابزار '{tool_name}' نه در مسیر مشخص شده و نه در PATH سیستم یافت نشد.")

    def backup(self) -> Path:
        """از mysqldump برای ایجاد بکاپ استفاده کرده و خروجی را با gzip فشرده می‌کند."""
        db_name = self.db_config['database']
        print(f"🚀 [{datetime.now()}] شروع پشتیبان‌گیری از MySQL: {db_name}...")

        mysqldump_path = self._get_tool_path("mysqldump")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_backup_path = self.temp_dir / f"{db_name}_{timestamp}.sql"
        compressed_backup_path = self.temp_dir / f"{db_name}_{timestamp}.sql.gz"

        command = [
            mysqldump_path,
            f"--host={self.db_config.get('host', 'localhost')}",
            f"--port={self.db_config.get('port', 3306)}",
            f"--user={self.db_config.get('user')}",
            f"--password={self.db_config.get('password')}",
            "--single-transaction",
            "--routines",
            "--triggers",
            db_name,
        ]

        try:
            with open(raw_backup_path, 'w', encoding='utf-8') as f:
                subprocess.run(command, check=True, stdout=f, text=True)

            with open(raw_backup_path, 'rb') as f_in, gzip.open(compressed_backup_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            print(f"✅ فایل بکاپ MySQL با موفقیت در '{compressed_backup_path}' ایجاد شد.")
            return compressed_backup_path
        except subprocess.CalledProcessError as e:
            # mysqldump خطاها را در stderr چاپ می‌کند اما ممکن است stdout هم داشته باشد
            error_output = e.stderr if e.stderr else e.stdout
            raise RuntimeError(f"پشتیبان‌گیری از MySQL شکست خورد: {error_output}") from e
        finally:
            if raw_backup_path.exists():
                raw_backup_path.unlink()

    def restore(self, backup_file_path: Path):
        """یک فایل بکاپ .sql را روی دیتابیس MySQL بازیابی می‌کند."""
        db_name = self.db_config['database']
        print(f"🔄 [{datetime.now()}] شروع فرآیند بازیابی دیتابیس MySQL: {db_name}...")

        mysql_path = self._get_tool_path("mysql")

        command = [
            mysql_path,
            f"--host={self.db_config.get('host', 'localhost')}",
            f"--port={self.db_config.get('port', 3306)}",
            f"--user={self.db_config.get('user')}",
            f"--password={self.db_config.get('password')}",
            db_name,
        ]

        try:
            with open(backup_file_path, 'r', encoding='utf-8') as f:
                subprocess.run(command, check=True, stdin=f, text=True)
            print(f"✅ بازیابی دیتابیس '{db_name}' با موفقیت کامل شد.")
        except subprocess.CalledProcessError as e:
            error_output = e.stderr if e.stderr else e.stdout
            raise RuntimeError(f"فرآیند بازیابی MySQL شکست خورد: {error_output}") from e