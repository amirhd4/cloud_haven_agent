from abc import ABC, abstractmethod
from pathlib import Path

class BaseDriver(ABC):
    """
    کلاس پایه انتزاعی برای تمام درایورهای دیتابیس.
    هر درایور جدید باید از این کلاس ارث‌بری کرده و متدهای آن را پیاده‌سازی کند.
    """
    def __init__(self, db_config: dict, temp_dir: Path):
        self.db_config = db_config
        self.temp_dir = temp_dir
        self.temp_dir.mkdir(exist_ok=True)

    @abstractmethod
    def backup(self) -> Path:
        """منطق پشتیبان‌گیری را اجرا کرده و مسیر فایل بکاپ فشرده را برمی‌گرداند."""
        pass

    @abstractmethod
    def restore(self, backup_file_path: Path):
        """یک فایل بکاپ استخراج شده (.sql) را روی دیتابیس بازیابی می‌کند."""
        pass