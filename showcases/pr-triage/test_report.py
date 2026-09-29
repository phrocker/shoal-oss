# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).parent))
import report
from shadow import digest,write_new

class ReportTest(unittest.TestCase):
    def fixture(self,root):
        protocol={'id':'p','mode':'retrospective_reconstruction'}
        unit={'id':'u','pr':1,'path':'p.go','symbol':'F','kind':'function'}
        manifest={'protocol_sha256':digest(protocol),'cases':[{'files':[{'path':'p.go'}],'units':[unit]}]}
        manifest['id']=digest(manifest)
        predictions={'manifest_id':manifest['id'],'predictions':[{'unit_id':'u','action':'full_review',
                    'optimization_eligible':False,'disposition':'predicted','baseline':'review',
                    'elapsed_ms':1,'answer':{'choice':'routine'}}]}
        predictions['id']=digest(predictions)
        for name,data in [('protocol',protocol),('manifest',manifest),('predictions',predictions)]:
            write_new(root/(name+'.json'),data)
        return unit
    def test_disagreement_is_not_counted_as_accuracy(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);unit=self.fixture(root)
            path=root/'assessor.json'
            write_new(path,{'status':'proposed','labels':[{'unit_id':'u','pr':1,'label':'review','witness':'changed grant check'}]})
            result=report.build(root,[path])
            self.assertEqual(result['assessors'][0]['eligible_cross_tab'],[{'model':'routine','proposed_reviewer':'review','count':1}])
            self.assertNotIn('accuracy',result);self.assertFalse(result['exclusion_enabled'])
    def test_missing_duplicate_or_foreign_labels_fail(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);unit=self.fixture(root)
            row={'unit_id':'u','pr':1,'label':'review','witness':'changed grant check'}
            for i,labels in enumerate(([],[row,row],[{**row,'unit_id':'other'}],[{**row,'pr':2}])):
                path=root/f'a{i}.json';write_new(path,{'status':'proposed','labels':labels})
                with self.assertRaises(ValueError):report.build(root,[path])
    def test_changed_protocol_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.fixture(root)
            (root/'protocol.json').write_text(json.dumps({'id':'different','mode':'retrospective_reconstruction'}))
            with self.assertRaises(ValueError):report.build(root,[])

if __name__=='__main__':unittest.main()
