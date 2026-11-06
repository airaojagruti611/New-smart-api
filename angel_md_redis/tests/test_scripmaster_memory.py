import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.scripmaster import load_scripmaster, _iter_master_rows, _options_by_underlying, resolve_eq_tokens


class ScripMasterMemoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'master.json'
        self.rows = [
            {'token':'1','symbol':'ABC-EQ','exch_seg':'NSE','name':'ABC','expiry':'','strike':'0'},
            {'token':'2','symbol':'ABC-BE','exch_seg':'NSE','name':'ABC','expiry':'','strike':'0'},
            {'token':'3','symbol':'ABC26NOV100CE','exch_seg':'NFO','name':'ABC','instrumenttype':'OPTSTK','expiry':'26NOV2026','strike':'10000'},
            {'token':'4','symbol':'ABCD-EQ','exch_seg':'NSE','name':'ABCD','expiry':'','strike':'0'},
            {'token':'5','symbol':'ABC','exch_seg':'BSE','name':'ABC','expiry':'','strike':'0'},
        ]
        self.path.write_text(json.dumps(self.rows), encoding='utf-8')

    def test_scoped_load_preserves_exact_equities_and_options(self):
        df = load_scripmaster(self.path, symbols=['abc'])
        self.assertEqual(set(df.token), {'1','2','3'})
        self.assertEqual(resolve_eq_tokens(df, ['ABC'])['ABC']['token'], '1')

    def test_history_load_needs_only_equities(self):
        df = load_scripmaster(self.path, symbols=['ABC'], equities_only=True)
        self.assertEqual(set(df.token), {'1','2'})

    def test_default_load_retains_all_records_and_fields(self):
        self.assertEqual(set(load_scripmaster(self.path).token), {'1','2','3','4','5'})

    def test_streaming_handles_chunk_boundaries_unicode_and_empty_results(self):
        rows = self.rows * 400
        rows[0] = {**rows[0], 'extra':'escaped " text ₹ ' * 6000}
        self.path.write_text(json.dumps(rows, ensure_ascii=False), encoding='utf-8')
        self.assertEqual(list(_iter_master_rows(self.path)), rows)
        self.assertTrue(load_scripmaster(self.path, symbols=['MISSING']).empty)

    def test_truncated_file_fails_instead_of_silently_dropping_rows(self):
        self.path.write_text('[{"token":"1"}, {"token":', encoding='utf-8')
        with self.assertRaises(ValueError):
            list(_iter_master_rows(self.path))

    def test_option_cache_is_reused_but_never_copied_as_pandas_metadata(self):
        df = load_scripmaster(self.path, symbols=['ABC'])
        groups = _options_by_underlying(df)
        self.assertIs(groups, _options_by_underlying(df))
        self.assertEqual(len(groups['ABC']), 1)
        self.assertNotIn('_opt_by_underlying', df.attrs)
        copied = df.copy()
        self.assertNotIn('_smartapi_option_groups', copied.__dict__)
