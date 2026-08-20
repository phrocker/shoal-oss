from __future__ import annotations

import os
import shutil
import unittest
from pathlib import Path

from sharkbite import Key, KeyValue, NativeAPI, RFileOperations


@unittest.skipUnless(os.environ.get("SHOAL_LIBRARY"), "SHOAL_LIBRARY is not set")
class CompiledStorageTests(unittest.TestCase):
    def test_rfile_named_locality_group_round_trip(self) -> None:
        api = NativeAPI(os.environ["SHOAL_LIBRARY"])
        directory = Path(__file__).resolve().parents[1] / "build" / "storage-test"
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        try:
            path = str(directory / "groups.rf")
            with RFileOperations.openForWrite(path, _api=api) as writer:
                writer.append(KeyValue(Key(b"z", b"default", b"", b"", 1), b"d"))
                writer.addLocalityGroup("named")
                writer.append(KeyValue(Key(b"a", b"named", b"", b"", 1), b"n"))
            with RFileOperations.sequentialRead(path, _api=api) as reader:
                self.assertEqual([entry.key.row for entry in reader], [b"a", b"z"])
        finally:
            shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
