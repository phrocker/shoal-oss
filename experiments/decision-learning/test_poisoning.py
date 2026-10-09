import unittest

import poisoning


class PoisoningCorpusTests(unittest.TestCase):
    def test_versioned_corpus_has_expected_attack_denominators(self):
        report = poisoning.run()
        self.assertEqual(report['corpus_id'], poisoning.CORPUS_ID)
        self.assertEqual(report['corpus_version'], 1)
        self.assertEqual(report['denominators']['attacks'], len(poisoning.ATTACKS))
        self.assertEqual(report['denominators']['rejected'], 5)
        self.assertEqual(report['denominators']['quarantined'], 1)
        self.assertEqual(report['denominators']['unsafe'], 0)
        self.assertEqual(report['denominators']['failed'], 0)
        self.assertEqual(report['clean_control']['status'], 'accepted')


if __name__ == '__main__':
    unittest.main()
