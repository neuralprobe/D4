import csv
from dataclasses import dataclass, field
import pandas as pd


class CSVHandler:
    """Class for handling CSV file operations."""

    @staticmethod
    def read_to_list(file_path):
        """Read the first column of a CSV file into a list."""
        with open(file_path, 'r') as file:
            return [row[0] for row in csv.reader(file)]

    @staticmethod
    def write_from_list(str_list, file_path):
        """Write a list of strings to a CSV file."""
        print(f"TRYING TO WRITE at {file_path}")
        print(str_list)
        with open(file_path, 'w', newline='') as file:
            print(f"WRITING ENTER at {file_path}")
            writer = csv.writer(file)
            writer.writerows([[line] for line in str_list])
            print(f"WRITING DONE at {file_path}")


class DataFrameUtils:
    """Utility class for DataFrame operations."""

    @staticmethod
    def append_inplace(df1, df2):
        """Append rows of one DataFrame to another in-place."""
        if df1.empty:
            for col in df2.columns:
                df1[col] = []
        for _, row in df2.iterrows():
            df1.loc[len(df1)] = row


@dataclass
class Time:
    """Class for managing time-related data."""
    timezone: str = 'America/New_York'
    start: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    end: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    current: pd.Timestamp = field(default_factory=pd.Timestamp.now)


class Tee:
    """Class to replicate stdout/stderr into multiple files."""
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)

    def flush(self):
        for f in self.files:
            f.flush()


class SingletonMeta(type):
    """A metaclass for Singleton pattern."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

    @classmethod
    def is_instantiated(cls, target_cls):
        """Check if a class is already instantiated."""
        return target_cls in cls._instances