import baostock as bs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path
import logging
import asyncio
import aiofiles
import concurrent.futures
import gzip
import json
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass
from tqdm import tqdm
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor
import threading


@dataclass
class StockConfig:
    cache_dir: Path
    start_date: str
    fields: List[str]
    max_workers: int = 5  # 并发数
    compression: bool = True  # 是否压缩存储
    check_integrity: bool = True  # 是否检查数据完整性
    retry_times: int = 3  # 重试次数
    retry_delay: int = 5  # 重试延迟（秒）


class DataIntegrityChecker:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def calculate_checksum(self, data: pd.DataFrame) -> str:
        """计算数据校验和"""
        return hashlib.md5(pd.util.hash_pandas_object(data).values).hexdigest()

    def verify_data(self, df: pd.DataFrame, trading_dates: Set[str]) -> Tuple[bool, str]:
        """验证数据完整性"""
        if df.empty:
            return False, "数据为空"

        # 检查必要列是否存在
        required_columns = {'date', 'open', 'high', 'low', 'close', 'volume'}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            return False, f"缺少必要列: {missing_columns}"

        # 检查数据类型
        try:
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='raise')
        except Exception as e:
            return False, f"数据类型错误: {str(e)}"

        # 检查日期连续性
        df_dates = set(df['date'].astype(str))
        missing_dates = trading_dates - df_dates
        if missing_dates:
            return False, f"缺少交易日数据: {len(missing_dates)} 天"

        return True, "数据完整性验证通过"


class DataCompressor:
    @staticmethod
    async def compress_data(data: pd.DataFrame, filepath: Path):
        """压缩数据并保存"""
        compressed_filepath = filepath.with_suffix('.gz')
        try:
            # 将 DataFrame 转换为 CSV 字符串
            csv_buffer = data.to_csv(index=False)
            # 使用 gzip 压缩并保存
            async with aiofiles.open(compressed_filepath, 'wb') as f:
                await f.write(gzip.compress(csv_buffer.encode('utf-8')))
            return True
        except Exception as e:
            logging.error(f"压缩数据失败 {filepath}: {str(e)}")
            return False

    @staticmethod
    async def decompress_data(filepath: Path) -> Optional[pd.DataFrame]:
        """解压数据并读取"""
        try:
            if not filepath.exists():
                return None

            async with aiofiles.open(filepath, 'rb') as f:
                content = await f.read()

            decompressed_content = gzip.decompress(content)
            return pd.read_csv(pd.io.common.StringIO(decompressed_content.decode('utf-8')))
        except Exception as e:
            logging.error(f"解压数据失败 {filepath}: {str(e)}")
            return None


class ProgressTracker:
    def __init__(self, total: int, desc: str):
        self.pbar = tqdm(total=total, desc=desc)
        self.lock = threading.Lock()
        self.failed_items: List[Tuple[str, str]] = []  # (stock_code, error_message)
        self.success_count = 0
        self.total_count = total

    def update_progress(self, success: bool, item: str = "", error: str = ""):
        with self.lock:
            self.pbar.update(1)
            if success:
                self.success_count += 1
            else:
                self.failed_items.append((item, error))

    def get_summary(self) -> dict:
        return {
            "total": self.total_count,
            "successful": self.success_count,
            "failed": len(self.failed_items),
            "failed_items": self.failed_items
        }

    def close(self):
        self.pbar.close()


class ChinaStockDataHandler:
    def __init__(self, config: StockConfig):
        self.config = config
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logger = self._setup_logger()
        self.integrity_checker = DataIntegrityChecker()
        self.trading_dates: Set[str] = set()
        self._setup_trading_dates()

        # 设置需要获取的字段
        self.default_fields = [
            "date", "code", "open", "high", "low", "close",
            "volume", "amount", "turn", "tradestatus", "pctChg",
            "peTTM", "pbMRQ", "psTTM", "circularMarketValue", "totalMarketValue"
        ]

        # 登录系统
        self._login()

    def _setup_logger(self) -> logging.Logger:
        log_dir = self.config.cache_dir / 'logs'
        log_dir.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f'stock_data_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(self.__class__.__name__)

    def _setup_trading_dates(self):
        """初始化交易日历"""
        try:
            self.trading_dates = set(self.get_trading_dates(
                self.config.start_date,
                datetime.now().strftime('%Y-%m-%d')
            ))
        except Exception as e:
            self.logger.error(f"获取交易日历失败: {str(e)}")
            self.trading_dates = set()

    def _login(self):
        """登录到 BaoStock"""
        for attempt in range(self.config.retry_times):
            try:
                bs.login()
                self.logger.info("Successfully logged in to BaoStock")
                return
            except Exception as e:
                if attempt < self.config.retry_times - 1:
                    self.logger.warning(f"登录失败，尝试重新登录 ({attempt + 1}/{self.config.retry_times})")
                    time.sleep(self.config.retry_delay)
                else:
                    raise Exception(f"登录失败: {str(e)}")

    def __del__(self):
        try:
            bs.logout()
            self.logger.info("Logged out from BaoStock")
        except:
            pass

    async def get_stock_data_async(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """异步获取股票数据"""
        for attempt in range(self.config.retry_times):
            try:
                fields = ",".join(self.default_fields)
                rs = bs.query_history_k_data_plus(
                    code, fields,
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="3"
                )

                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

                if data_list:
                    df = pd.DataFrame(data_list, columns=self.default_fields)
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount',
                                    'turn', 'pctChg', 'peTTM', 'pbMRQ', 'psTTM',
                                    'circularMarketValue', 'totalMarketValue']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    return df
                return None

            except Exception as e:
                if attempt < self.config.retry_times - 1:
                    self.logger.warning(f"获取数据失败 {code}，尝试重试 ({attempt + 1}/{self.config.retry_times})")
                    await asyncio.sleep(self.config.retry_delay)
                else:
                    raise Exception(f"获取数据失败: {str(e)}")

    async def process_single_stock(self, stock_code: str, progress_tracker: ProgressTracker) -> bool:
        """处理单个股票的数据更新"""
        cache_file = self.config.cache_dir / f"{stock_code}_daily.{'gz' if self.config.compression else 'csv'}"
        today = datetime.now().strftime('%Y-%m-%d')

        try:
            existing_data = None
            if cache_file.exists():
                if self.config.compression:
                    existing_data = await DataCompressor.decompress_data(cache_file)
                else:
                    existing_data = pd.read_csv(cache_file)

            if existing_data is not None and not existing_data.empty:
                last_date = existing_data['date'].max()
                new_data = await self.get_stock_data_async(stock_code, last_date, today)

                if new_data is not None and not new_data.empty:
                    df = pd.concat([existing_data, new_data]).drop_duplicates(subset=['date'])
                else:
                    df = existing_data
            else:
                df = await self.get_stock_data_async(stock_code, self.config.start_date, today)
                if df is None or df.empty:
                    raise Exception("无法获取数据")

            # 数据完整性检查
            if self.config.check_integrity:
                is_valid, message = self.integrity_checker.verify_data(df, self.trading_dates)
                if not is_valid:
                    raise Exception(f"数据完整性检查失败: {message}")

            # 保存数据
            if self.config.compression:
                await DataCompressor.compress_data(df, cache_file)
            else:
                df.to_csv(cache_file, index=False)

            progress_tracker.update_progress(True)
            return True
