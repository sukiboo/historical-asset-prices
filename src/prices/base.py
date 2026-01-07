import logging
import os
from typing import cast

import pandas as pd
from tqdm import tqdm

from ..utils import (
    compute_file_md5,
    create_empty_marker,
    get_file_from_s3,
    get_flat_file_path,
    save_flat_file_bytes,
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
    ) -> None:
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

    def format_info_line(self, downloaded: int, updated: int, skipped: int, date: str) -> str:
        """Format the info line with current counters and date."""
        info = (
            f"Retrieving {self.asset_type} files for {date}: "
            f"downloaded={downloaded}, updated={updated}, skipped={skipped}"
        )
        return info

    def retrieve_prices(self) -> None:
        """Download flat files from S3 and cache them locally."""
        if self.date_start >= self.date_end:
            logger.info(
                f"Date range is before {self.asset_type} availability start date "
                f"{self.available_from.date()}, skipping retrieval"
            )
            return

        fetch_aggs = with_retry(get_file_from_s3)

        aws_access_key_id = os.getenv("MASSIVE_AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("MASSIVE_API_KEY")

        downloaded, updated, skipped = 0, 0, 0
        print(
            self.format_info_line(
                downloaded, updated, skipped, self.date_start.strftime("%Y-%m-%d")
            )
        )
        with tqdm(
            total=(self.date_end - self.date_start).days,
            desc="",
            unit="day",
            leave=True,
        ) as pbar:
            current_day: pd.Timestamp = self.date_start

            while current_day < self.date_end:
                # Construct S3 path
                s3_path = (
                    f"{self.s3_prefix}/minute_aggs_v1/"
                    f'{current_day.strftime("%Y/%m")}/'
                    f'{current_day.strftime("%Y-%m-%d")}.csv.gz'
                )

                flat_file_path = get_flat_file_path(self.files_dir, current_day)
                empty_marker_path = f"{flat_file_path}.empty"
                file_exists_locally = os.path.exists(flat_file_path)

                # Get local ETag if file exists (for conditional GET)
                local_etag = None
                if file_exists_locally:
                    local_hash = compute_file_md5(flat_file_path)
                    local_etag = local_hash

                # Use conditional GET - S3 returns 304 if file unchanged
                file_bytes, s3_etag, file_exists_in_s3 = fetch_aggs(
                    object_key=s3_path,
                    bucket_name="flatfiles",
                    logger=logger,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    if_none_match=local_etag,
                )

                if file_bytes is None and file_exists_in_s3:
                    # File unchanged (304 Not Modified)
                    skipped += 1
                    logger.debug(f"File up to date: {flat_file_path}")
                elif not file_exists_in_s3:
                    # File doesn't exist in S3 (weekend/holiday)
                    if not os.path.exists(empty_marker_path):
                        create_empty_marker(flat_file_path, logger)
                    else:
                        logger.debug(f"Empty marker already exists: {empty_marker_path}")
                elif file_bytes is not None:
                    # File was downloaded (new or updated)
                    if file_exists_locally:
                        updated += 1
                        logger.debug(f"Updating file: {flat_file_path}")
                    else:
                        downloaded += 1
                        logger.debug(f"Downloading file: {flat_file_path}")

                    save_flat_file_bytes(file_bytes, flat_file_path, logger)

                # Update info line every iteration (counters always change)
                # Use tqdm.write which properly handles output when using progress bars
                date_str = current_day.strftime("%Y-%m-%d")
                pbar.write(
                    f"\033[1A\033[K{self.format_info_line(downloaded, updated, skipped, date_str)}"
                )  # Move up, clear line, write new

                pbar.update(1)
                current_day = cast(pd.Timestamp, current_day + pd.Timedelta(days=1))

        # Log summary
        logger.info(
            f"{self.asset_type.capitalize()} files summary: "
            f"{downloaded} downloaded, {updated} updated, {skipped} skipped\n"
        )
