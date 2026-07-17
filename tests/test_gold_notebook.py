import json
from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "gold" / "modelo_estrella_parquet_powerbi.ipynb"


class GoldNotebookTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
        cls.code = "\n".join(
            "".join(cell["source"])
            for cell in cls.notebook["cells"]
            if cell["cell_type"] == "code"
        )

    def test_all_code_cells_compile(self):
        for index, cell in enumerate(self.notebook["cells"], start=1):
            if cell["cell_type"] == "code":
                compile("".join(cell["source"]), f"gold-cell-{index}", "exec")

    def test_expected_star_tables_are_present(self):
        expected_tables = {
            "fact_viajes_agregados",
            "dim_tiempo",
            "dim_hora",
            "dim_tipo_taxi",
            "dim_zona_pickup",
            "dim_zona_dropoff",
            "dim_pago",
            "dim_ratecode",
            "dim_trip_type",
            "dim_shared_ride",
            "dim_proveedor",
        }
        for table in expected_tables:
            self.assertIn(table, self.code)

    def test_notebook_is_parquet_only(self):
        self.assertNotIn("from pymongo", self.code)
        self.assertNotIn("MongoClient", self.code)
        self.assertIn("process_manifest.json", self.code)
        self.assertIn('.write.mode("overwrite").parquet', self.code)


if __name__ == "__main__":
    unittest.main()
