from dataclasses import dataclass
from pathlib import Path
import sys
import offline.tasks as tasks
from pyspark.sql import DataFrame

# Allow direct execution from any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from offline.common import CommonPipeline


@dataclass
class PrefillPipeline(CommonPipeline):
    """
    1. Đọc xml và tạo dataframe
    2. Load commonpipeline.
    """

    def load_xml_and_create_dataframe(self, xml: str) -> DataFrame:
        return tasks.create_article_dataframe(xml)

    def run(self):
        df = self.load_xml_and_create_dataframe
        # TODO: Nói chung chỉ cần khai báo một số đường dẫn là xong
        super().run()
