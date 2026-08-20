from __future__ import annotations

import os
import unittest
from concurrent.futures import ThreadPoolExecutor

from sharkbite import LoggingConfiguration, Mutation, NativeAPI


class CompiledLibraryTests(unittest.TestCase):
    @unittest.skipUnless(
        os.environ.get("SHOAL_LIBRARY"), "SHOAL_LIBRARY is not set"
    )
    def test_writer_and_administration_symbols_match_reported_capabilities(self):
        api = NativeAPI()
        api.require(6, 7, 8, 9, 10, 11, 12, 19, 29, 30)
        self.assertGreaterEqual(api.info.version, (1, 18, 0))
        with Mutation(b"row", _api=api) as mutation:
            mutation.put(b"cf", b"cq", b"A", 7, b"value")
            mutation.delete(b"cf", b"cq", b"A", 8)
            self.assertGreater(mutation.size(), 0)
        LoggingConfiguration._set(1, api)
        self.assertEqual(api.lib.shoal_logging_get_level(), 1)
        LoggingConfiguration._set(0, api)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: api.require(9, 10, 11, 12, 19), range(128)))


if __name__ == "__main__":
    unittest.main()
