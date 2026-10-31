"""Quick standard-library tests for JSONL export parsing and batched upsert.
Run with the virtualenv where PyMongo is installed:
    python -m unittest discover -s tests -v
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock
from pymongo.operations import ReplaceOne

from src.export_mongodb import track_recommendation_documents, upsert_batches


class ExportTests(unittest.TestCase):
    def test_jsonl_reading(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / 'part-00000.jsonl'
            p.write_text(json.dumps({'track_id': 12, 'recommendations': [
                {'rank': 1, 'track_id': 19, 'similarity': 0.71}]}) + '\n', encoding='utf-8')
            data = list(track_recommendation_documents(t))
            self.assertEqual(data[0]['_id'], 12)
            self.assertEqual(data[0]['recommendations'][0]['track_id'], 19)

    def test_batching(self):
        coll = MagicMock()
        coll.name = 'unit-test'
        docs = [{'_id': i, 'x': i} for i in range(5)]
        written = upsert_batches(coll, iter(docs), batch_size=2)
        self.assertEqual(written, 5)
        self.assertEqual(coll.bulk_write.call_count, 3)
        for call in coll.bulk_write.call_args_list:
            self.assertTrue(all(isinstance(x, ReplaceOne) for x in call.args[0]))


if __name__ == '__main__':
    unittest.main()
