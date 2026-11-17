from dataclasses import dataclass
from pathlib import Path
import sys

from pyspark.sql import DataFrame

# Allow direct execution from any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import CommonPipeline


@dataclass
class PrefillPipeline(CommonPipeline):
    """
    1. Đọc xml và tạo dataframe
    2. Load commonpipeline.
    """
    def load_xml_and_create_dataframe(self, xml: str) -> DataFrame:
        raise NotImplementedError
