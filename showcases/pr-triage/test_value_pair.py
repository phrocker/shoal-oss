# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import paid_review
from value_pair import continue_budget, eligible_metadata, run_pair
from shadow import digest


class ContinuationTests(unittest.TestCase):
    def test_carryover_keeps_spend_and_prevents_reset_or_duplicate_import(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);parent=root/'parent.sqlite';dest=root/'next.sqlite'
            old={'id':'old','budget':{'limit_usd':25,'maximum_calls':8,'call_reservation_usd':3,'cli_max_budget_usd':1.5}}
            db=paid_review.connect(parent,old)
            for i in range(4):paid_review.reserve(db,old,str(i),'prompt');paid_review.settle(db,str(i),.25)
            db.close()
            new={'id':'next','parent_protocol_id':'old','parent_budget_sha256':digest(parent.read_bytes()),'carryover_paid_usd':1,
                 'budget':dict(old['budget'],maximum_calls=12)}
            continue_budget(parent,dest,new)
            db=paid_review.connect(dest,new);self.assertEqual(paid_review.receipt(db,new)['reported_paid_usd'],1)
            self.assertEqual(len(paid_review.receipt(db,new)['calls']),4);db.close()
            with self.assertRaises(FileExistsError):continue_budget(parent,dest,new)
            new['carryover_paid_usd']=0
            with self.assertRaisesRegex(ValueError,'charge mismatch'):continue_budget(parent,root/'bad.sqlite',new)
    def test_future_ready_source_pin_required(self):
        p={'registered_at_utc':'2026-10-03T00:00:00+00:00','excluded_families':[]}
        case={'pr':500,'head':'h','files':[{'path':'x.go'}]}
        m={'number':500,'created_at':'2026-10-03T01:00:00+00:00','state':'open','draft':False,'head':{'sha':'h','ref':'fix/example'}}
        eligible_metadata(m,p,case)
        for field,value in [('draft',True),('state','closed'),('created_at','2026-10-02T00:00:00+00:00')]:
            with self.subTest(field=field),self.assertRaises(ValueError):eligible_metadata(dict(m,**{field:value}),p,case)
    def test_rehearsal_cannot_trigger_paid_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);protocol={'budget':{}};protocol['id']=digest(protocol)
            (root/'protocol.json').write_text(json.dumps(protocol))
            packets={'protocol_id':protocol['id'],'status':'rehearsal_no_paid_calls'};packets['id']=digest(packets)
            (root/'packets.json').write_text(json.dumps(packets))
            with patch('value_pair.subprocess.run') as paid:
                with self.assertRaisesRegex(ValueError,'not eligible'):
                    run_pair(root,root/'protocol.json',root/'budget',root/'claude',root,root/'model',root/'extractor')
                paid.assert_not_called()


if __name__=='__main__':unittest.main()
