import logging
import os
from typing import cast

import pandas as pd
from tqdm import tqdm

from ..utils import (
    create_empty_marker,
    get_file_from_s3,
    get_flat_file_path,
    save_flat_file,
    with_retry,
)

logger = logging.getLogger(__name__)


class BasePrices:
    """Base class for retrieving raw flat files from S3."""

    def __init__(
        self,
        date_start: pd.Timestamp,
        date_end: pd.Timestamp,
        asset_type: str,
        s3_prefix: str,
        available_from: str,
        tickers: list[str] | None = None,
    ) -> None:
        self.tickers = [ticker.upper() for ticker in (tickers or [])]
        self.available_from = pd.Timestamp(available_from)
        self.date_start = max(date_start, self.available_from)
        self.date_end = date_end
        self.asset_type = asset_type
        self.s3_prefix = s3_prefix
        self.files_dir = f"files/{asset_type}"

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"date_start={self.date_start.date()}, "
            f"date_end={self.date_end.date()})"
        )

    def retrieve_prices(self) -> None:
        """Download flat files from S3 and cache them locally."""
        if self.date_start >= self.date_end:
            logger.info(
                f"Date range is before {self.asset_type} availability start date "
                f"{self.available_from.date()}, skipping retrieval"
            )
            return

        fetch_aggs = with_retry(get_file_from_s3)

        with tqdm(
            total=(self.date_end - self.date_start).days,
            desc=f"Retrieving {self.asset_type} files",
            unit="day",
        ) as pbar:
            current_day: pd.Timestamp = self.date_start
            while current_day < self.date_end:
                pbar.set_postfix({"date": current_day.strftime("%Y-%m-%d")})

                # Check if flat file exists locally first
                flat_file_path = get_flat_file_path(self.files_dir, current_day)
                empty_marker_path = f"{flat_file_path}.empty"

                if os.path.exists(flat_file_path):
                    logger.debug(f"File already cached: {flat_file_path}")
                elif os.path.exists(empty_marker_path):
                    logger.debug(f"Empty marker exists: {empty_marker_path}")
                else:
                    # Download from S3 and cache locally
                    s3_path = (
                        f"{self.s3_prefix}/minute_aggs_v1/"
                        f'{current_day.strftime("%Y/%m")}/'
                        f'{current_day.strftime("%Y-%m-%d")}.csv.gz'
                    )
                    data = fetch_aggs(
                        object_key=s3_path,
                        bucket_name="flatfiles",
                        logger=logger,
                        aws_access_key_id=os.getenv("MASSIVE_AWS_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.getenv("MASSIVE_API_KEY"),
                    )

                    # Save flat file or empty marker for future use
                    if not data.empty:
                        save_flat_file(data, flat_file_path, logger)
                    else:
                        create_empty_marker(flat_file_path, logger)

                pbar.update(1)
                current_day = cast(pd.Timestamp, current_day + pd.Timedelta(days=1))
