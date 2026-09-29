# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('shadow',Path(__file__).with_name('shadow.py'))
shadow=importlib.util.module_from_spec(spec);spec.loader.exec_module(shadow)

class ShadowTest(unittest.TestCase):
    def test_paths_preserve_rename_delete_and_unusual_names(self):
        paths=shadow.changed_paths(b'R100\0old name.go\0new\nname.go\0D\0deleted.go\0A\0new.go\0M\0policy.yaml\0')
        self.assertEqual(paths[0]['before_path'],'old name.go')
        self.assertEqual(paths[0]['after_path'],'new\nname.go')
        self.assertIsNone(paths[1]['after_path']);self.assertIsNone(paths[2]['before_path'])
        self.assertEqual(len(paths),4)
    def test_unchanged_function_omitted_but_changed_constant_retained(self):
        fn={'key':'function:F','kind':'function','text':'func F() {}'}
        const={'key':'const:Role','kind':'const','text':'const Role=1'}
        before={'declarations':[fn,const],'residue':'package p'}
        after={'declarations':[fn,{**const,'text':'const Role=2'}],'residue':'package p'}
        paired=shadow.pair_declarations(before,after)
        self.assertEqual(len(paired),1);self.assertEqual(paired[0]['kind'],'const')
    def test_duplicate_keys_rejected(self):
        decl={'key':'function:init','kind':'function','text':'func init(){}'}
        with self.assertRaises(ValueError):
            shadow.pair_declarations({'declarations':[decl,decl],'residue':''},{'declarations':[],'residue':''})
    def test_rounding_accepted_invalid_mass_and_nan_rejected(self):
        def response(probs):return {'answers':{'boundary':{'type':'choice','choice':'a','probabilities':probs}}}
        self.assertEqual(shadow.validate_answer(response({'a':.3333,'b':.3333,'c':.3333}),{'a':1,'b':2,'c':3})['choice'],'a')
        for probabilities in ({'a':.9,'b':.9,'c':.1},{'a':float('nan'),'b':0,'c':0},{'a':True,'b':0,'c':0},{'a':1,'b':0}):
            with self.assertRaises(ValueError):shadow.validate_answer(response(probabilities),{'a':1,'b':2,'c':3})
    def test_observation_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'record.json';shadow.write_new(path,{'value':1})
            with self.assertRaises(FileExistsError):shadow.write_new(path,{'value':2})

if __name__=='__main__':unittest.main()
