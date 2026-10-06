from pathlib import Path
import pandas as pd
from logger_config import get_logger

logger = get_logger("UsageProcessor")
# =========================================================
# Application Logging
# =========================================================

# BASE_DIR = Path(__file__).resolve().parent.parent
# LOG_DIR = BASE_DIR / "logs"
# LOG_DIR.mkdir(parents=True, exist_ok=True)

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
#     handlers=[
#         logging.FileHandler(
#             LOG_DIR / "application.log",
#             encoding="utf-8"
#         ),
#         logging.StreamHandler()
#     ]
# )

# logger = logging.getLogger("UsageProcessor")


class UsageProcessor:
    """Reusable processor for telecom activity data."""

    REQUIRED_COLUMNS = [
        "timestamp", "grid_id", "country_code",
        "sms_in", "sms_out", "call_in", "call_out",
        "internet_activity"
    ]

    ACTIVITY_COLUMNS = [
        "sms_in", "sms_out", "call_in",
        "call_out", "internet_activity"
    ]

    def __init__(self,file_path=None,df=None):
        if file_path is None and df is None:
            raise ValueError("Provide either file_path or df.")
        if file_path is not None and df is not None:
            raise ValueError("Provide either file_path or df, not both.")
        self.file_path = file_path
        self.raw_df = df.copy(deep=True) if df is not None else None
        self.df = None
        self.rejected_df = pd.DataFrame()
        self.grid_hour_data = None
        self.daily_summary = None
        self.grid_summary = None
        self.input_rows = 0
        self.rejected_rows = 0
        self.nulls_handled = 0
        self.output_rows = 0

        self.logger = logger

    def load_data(self):
        """Load input data from DataFrame or CSV."""
        if self.raw_df is None:
            if self.file_path is None:
                raise ValueError("No file path or DataFrame provided.")

            path = Path(self.file_path)

            if not path.exists():
                raise FileNotFoundError(f"Input file not found: {path}")

            self.raw_df = pd.read_csv(path)

            self.logger.info("input_path=%s", path)

        self.input_rows = len(self.raw_df)

        if self.input_rows == 0:
            raise ValueError("Input data contains zero rows.")

        self.logger.info("input_rows=%d", self.input_rows)
        self.logger.info("VALIDATION PASSED: load_data()")

        return self.raw_df

    def clean_data(self):
        """Create canonical curated data and apply validation rules."""
        if self.raw_df is None:
            raise ValueError("Run load_data() before clean_data().")

        column_mapping = {
            "datetime": "timestamp",
            "CellID" : "grid_id",
            "countrycode":"country_code",
            "smsin":"sms_in",
            "smsout":"sms_out",
            "callin":"call_in",
            "callout":"call_out",
            "internet":"internet_activity"
        }

        self.df = self.raw_df.rename(columns=column_mapping).copy(deep=True)
    
        missing = [
            c for c in self.REQUIRED_COLUMNS
            if c not in self.df.columns
        ]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")


        self.df["timestamp"] = pd.to_datetime(
            self.df["timestamp"], errors="coerce"
        )

        # Reject invalid timestamp/grid_id
        invalid_keys = (
            self.df["timestamp"].isna()
            | self.df["grid_id"].isna()
        )

        rejected_keys = self.df.loc[invalid_keys].copy()
        self.df = self.df.loc[~invalid_keys].copy()

        # Reject negative activity
        negative = self.df[self.ACTIVITY_COLUMNS].lt(0).any(axis=1)

        rejected_negative = self.df.loc[negative].copy()
        self.df = self.df.loc[~negative].copy()

        # Keep rejected records for audit
        self.rejected_df = pd.concat(
            [rejected_keys, rejected_negative],
            ignore_index=True
        )
        self.rejected_rows = len(self.rejected_df)

        # Curated-layer null → zero
        nulls = self.df[self.ACTIVITY_COLUMNS].isna()
        self.nulls_handled = int(nulls.sum().sum())

        self.df[self.ACTIVITY_COLUMNS] = (
            self.df[self.ACTIVITY_COLUMNS].fillna(0)
        )

        # Validation
        if self.df["grid_id"].isna().any():
            raise AssertionError("Cleaned data contains missing grid_id.")

        if self.df["timestamp"].isna().any():
            raise AssertionError("Cleaned data contains missing timestamp.")

        if self.df[self.ACTIVITY_COLUMNS].lt(0).any().any():
            raise AssertionError("Cleaned data contains negative activity.")

        if self.df[self.ACTIVITY_COLUMNS].isna().any().any():
            raise AssertionError("Activity nulls remain after cleaning.")

        self.logger.info("rows_rejected=%d", self.rejected_rows)
        self.logger.info("nulls_handled=%d", self.nulls_handled)
        self.logger.info("VALIDATION PASSED: clean_data()")

        return self.df

    def derive_time_features(self):
        """Derive date, hour and day_of_week."""
        if self.df is None:
            raise ValueError("Run clean_data() first.")

        ts = self.df["timestamp"]
        self.df["date"] = ts.dt.date
        self.df["hour"] = ts.dt.hour
        self.df["day_of_week"] = ts.dt.day_name()

        required = ["date", "hour", "day_of_week"]
        missing = [c for c in required if c not in self.df.columns]

        if missing:
            raise AssertionError(f"Missing time features: {missing}")

        self.logger.info(
            "VALIDATION PASSED: derive_time_features()"
        )

        return self.df

    def derive_activity_features(self):
        """Create project-defined activity measures."""
        if self.df is None:
            raise ValueError("Run clean_data() first.")

        self.df["total_sms"] = (
            self.df["sms_in"] + self.df["sms_out"]
        )
        self.df["total_calls"] = (
            self.df["call_in"] + self.df["call_out"]
        )
        self.df["total_activity"] = (
            self.df["total_sms"]
            + self.df["total_calls"]
            + self.df["internet_activity"]
        )

        required = [
            "total_sms",
            "total_calls",
            "total_activity"
        ]
        missing = [c for c in required if c not in self.df.columns]

        if missing:
            raise AssertionError(
                f"Missing activity features: {missing}"
            )

        self.logger.info(
            "VALIDATION PASSED: derive_activity_features()"
        )

        return self.df

    def aggregate_to_grid_time(self):
        """Aggregate country-code rows to timestamp + grid_id."""
        if self.df is None:
            raise ValueError("Run clean_data() first.")

        required = [
            "total_sms",
            "total_calls",
            "internet_activity",
            "total_activity"
        ]
        missing = [c for c in required if c not in self.df.columns]

        if missing:
            raise ValueError(
                "Run derive_activity_features() first. "
                f"Missing: {missing}"
            )

        input_rows = len(self.df)

        self.grid_hour_data = (
            self.df
            .groupby(
                ["timestamp", "grid_id"],
                as_index=False
            )
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                internet_activity=("internet_activity", "sum"),
                total_activity=("total_activity", "sum")
            )
        )

        self.output_rows = len(self.grid_hour_data)

        # Acceptance checks
        if self.grid_hour_data.duplicated(
            ["grid_id", "timestamp"]
        ).any():
            raise AssertionError(
                "Grid-hour output contains duplicates."
            )

        if self.output_rows >= input_rows:
            raise AssertionError(
                "Grid-hour aggregation did not reduce "
                "the number of rows."
            )

        if "country_code" in self.grid_hour_data.columns:
            raise AssertionError(
                "country_code leaked into grid/hour analytics."
            )

        # Required audit logging
        self.logger.info("input_rows=%d", input_rows)
        self.logger.info("rows_rejected=%d", self.rejected_rows)
        self.logger.info("nulls_handled=%d", self.nulls_handled)
        self.logger.info("output_rows=%d", self.output_rows)
        self.logger.info(
            "VALIDATION PASSED: aggregate_to_grid_time()"
        )

        return self.grid_hour_data

    def compute_kpis(self):
        """Compute daily and grid-level summaries."""
        if self.grid_hour_data is None:
            raise ValueError(
                "Run aggregate_to_grid_time() first."
            )

        metrics = {
            "total_sms": ("total_sms", "sum"),
            "total_calls": ("total_calls", "sum"),
            "internet_activity": ("internet_activity", "sum"),
            "total_activity": ("total_activity", "sum")
        }

        daily = self.grid_hour_data.copy()
        daily["date"] = daily["timestamp"].dt.date

        self.daily_summary = (
            daily
            .groupby("date", as_index=False)
            .agg(
                **metrics,
                active_grids=("grid_id", "nunique")
            )
        )

        self.grid_summary = (
            self.grid_hour_data
            .groupby("grid_id", as_index=False)
            .agg(**metrics)
        )

        if "country_code" in self.daily_summary.columns:
            raise AssertionError(
                "country_code leaked into daily summary."
            )

        if "country_code" in self.grid_summary.columns:
            raise AssertionError(
                "country_code leaked into grid summary."
            )

        self.logger.info(
            "VALIDATION PASSED: compute_kpis()"
        )

        return self.daily_summary, self.grid_summary

    def export_summary(self, output_dir="outputs"):
        """Export daily and grid-level summary tables."""
        if self.daily_summary is None or self.grid_summary is None:
            raise ValueError(
                "Run compute_kpis() before exporting."
            )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        daily_path = output_path / "daily_summary.csv"
        grid_path = output_path / "grid_summary.csv"

        self.daily_summary.to_csv(daily_path, index=False)
        self.grid_summary.to_csv(grid_path, index=False)

        if not daily_path.exists():
            raise AssertionError(
                "daily_summary.csv was not created."
            )

        if not grid_path.exists():
            raise AssertionError(
                "grid_summary.csv was not created."
            )

        self.logger.info(
            "daily_summary_output=%s", daily_path
        )
        self.logger.info(
            "grid_summary_output=%s", grid_path
        )
        self.logger.info(
            "VALIDATION PASSED: export_summary()"
        )

        return daily_path, grid_path


if __name__ == "__main__":
    processor = UsageProcessor(file_path="D:\\Network Operations Predictive System\\data\\sms-call-internet-mi-2013-11-01.csv")
    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.derive_activity_features()
    processor.aggregate_to_grid_time()
    processor.compute_kpis()
    processor.export_summary(output_dir="outputs")