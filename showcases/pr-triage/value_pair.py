#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
"""Prepare full-population frozen-classifier pairs and preserve cumulative budget."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import tempfile
from candidates import checked, original_state, change_text
from shadow import canonical, digest, write_new
import paid_review
import paired_review
import paired_report


def continue_budget(parent, destination, protocol):
    raw=parent.read_bytes()
    if digest(raw)!=protocol['parent_budget_sha256']:raise ValueError('parent budget changed; reconcile before continuation')
    db=sqlite3.connect(parent.resolve().as_uri()+'?mode=ro',uri=True)
    try:
        if db.execute('SELECT id FROM experiment').fetchall()!=[(protocol['parent_protocol_id'],)]:raise ValueError('parent protocol mismatch')
        rows=db.execute('SELECT charged,status FROM calls').fetchall()
        if len(rows)!=4 or any(status!='settled' for _,status in rows):raise ValueError('parent calls must be reconciled')
        if sum(cost for cost,_ in rows)!=paid_review.micros(protocol['carryover_paid_usd']):raise ValueError('carryover charge mismatch')
    finally:db.close()
    with destination.open('xb') as out:out.write(raw)
    db=sqlite3.connect(destination)
    try:db.execute('UPDATE experiment SET id=?',(protocol['id'],));db.commit()
    finally:db.close()


def build_code_inputs(manifest,model):
    rows=[]
    if model['recipe']!=['code','linear_svm']:raise ValueError('continuation requires frozen code SVM')
    for case in manifest['cases']:
        for u in case['units']:
            if u['kind']!='function':continue
            state=original_state(u)
            rows.append({'unit_id':u['id'],'pr':u['pr'],'path':u['path'],'symbol':u['symbol'],'label':'unknown',
                'text':{'code':canonical(state)},'target_body_sha256':[digest(state[s].encode()) for s in ('before','after') if state[s]],
                'code_state_sha256':digest(state),'diff_state_sha256':digest(change_text(u)),
                'source_bytes':sum(len(state[s].encode()) for s in ('before','after')),'diff_bytes':len(change_text(u).encode())})
    if len({r['unit_id'] for r in rows})!=len(rows):raise ValueError('duplicate function IDs')
    out={'sources':[{'manifest_id':manifest['id'],'assessment_sha256':None}], 'rows':rows,
         'evidence_contract':model['evidence_contract'],'builder_sha256':digest(Path(__file__).read_bytes()),
         'mode':'Code-only projection. Training dependency contract retained; operation features neither constructed nor used.',
         'optimization_enabled':False};out['id']=digest(out);return out


def eligible_metadata(metadata,protocol,case):
    registered=datetime.fromisoformat(protocol['registered_at_utc'])
    created=datetime.fromisoformat(metadata['created_at'])
    if created.tzinfo is None or created<=registered:raise ValueError('PR predates future-only protocol')
    if metadata['state']!='open' or metadata['draft'] is not False:raise ValueError('PR must be open and ready')
    if metadata['number']!=case['pr'] or case['pr'] in protocol['excluded_families']:
        raise ValueError('PR family excluded or mismatched')
    if metadata['head']['sha']!=case['head'] or metadata['head']['ref'].startswith('experiment/'):
        raise ValueError('PR head mismatch or experiment branch')
    if not any(f['path'].endswith('.go') for f in case['files']):raise ValueError('no changed Go files')


def prepare(repo,manifest_path,model_path,extractor,protocol,output,metadata=None,rehearsal=False):
    manifest=checked(manifest_path);model=checked(model_path)
    if digest(model_path.read_bytes())!=protocol['model_sha256'] or model['id']!=protocol['model_id'] or model['threshold']!=protocol['threshold']:
        raise ValueError('frozen model mismatch')
    if len(manifest['cases'])!=1:raise ValueError('prepare exactly one pinned PR pair')
    if not rehearsal:
        if metadata is None:raise ValueError('pinned PR metadata required')
        eligible_metadata(metadata,protocol,manifest['cases'][0])
    output.mkdir()
    write_new(output/'source-manifest.json',manifest)
    if metadata is not None:write_new(output/'pr-metadata.json',metadata)
    feature_started=time.perf_counter();inputs=build_code_inputs(manifest,model);feature_seconds=time.perf_counter()-feature_started
    write_new(output/'inputs.json',inputs)
    selection={'model_id':model['id'],'inputs_id':inputs['id'],'protocol_id':protocol['id']};selection['id']=digest(selection);write_new(output/'classifier-selection.json',selection)
    start=time.perf_counter()
    subprocess.run([sys.executable,str(Path(__file__).with_name('replay_operation_model.py')),'--inputs',str(output/'inputs.json'),
                    '--model',str(model_path),'--selection',str(output/'classifier-selection.json'),'--output',str(output/'classifier-predictions.json')],check=True)
    elapsed=time.perf_counter()-start
    pred=checked(output/'classifier-predictions.json');indexed={r['unit_id']:r for r in pred['predictions']}
    bridge={'model_id':model['id'],'parent_prediction_id':pred['id'],'validation_manifest_id':manifest['id'],'threshold':model['threshold'],'optimization_enabled':False};bridge['id']=digest(bridge)
    rows=[indexed.get(u['id'],{'unit_id':u['id'],'disposition':'unsupported_unit','review_score':None,'proposal':'retain','action':'full_review','optimization_eligible':False}) for c in manifest['cases'] for u in c['units']]
    shadow={'manifest_id':manifest['id'],'selection_id':bridge['id'],'predictions':rows,'optimization_enabled':False}
    packets=paired_review.prepare(repo,manifest,shadow,extractor,bridge)
    write_new(output/'shadow-selection.json',bridge);shadow['id']=digest(shadow);write_new(output/'shadow-predictions.json',shadow)
    for packet in packets:(output/f"pr-{packet['pr']}-{packet['arm']}.txt").write_text(packet.pop('prompt'))
    receipt={'protocol_id':protocol['id'],'manifest_id':manifest['id'],'model_id':model['id'],'packets':packets,
        'feature_construction_seconds':feature_seconds,'scoring_subprocess_seconds':elapsed,'model_batch_seconds':pred['elapsed_seconds'],'real_review_action':'full_review','optimization_enabled':False,
        'preparer_sha256':digest(Path(__file__).read_bytes()),'packet_builder_sha256':digest(Path(paired_review.__file__).read_bytes()),
        'status':'rehearsal_no_paid_calls' if rehearsal else 'oversized_non_evaluable' if any(p['prompt_bytes']>protocol['budget']['prompt_byte_limit'] for p in packets) else 'ready'}
    receipt['id']=digest(receipt);write_new(output/'packets.json',receipt)
    return receipt


def run_pair(run,protocol_path,budget,claude,repo,model,extractor):
    protocol=checked(protocol_path);packets=checked(run/'packets.json')
    if packets['protocol_id']!=protocol['id'] or packets['status']!='ready':raise ValueError('pair not eligible for calls')
    if len(packets['packets'])!=2 or {p['arm'] for p in packets['packets']}!={'full','candidate'}:raise ValueError('incomplete pair')
    manifest=checked(run/'source-manifest.json')
    eligible_metadata(json.loads((run/'pr-metadata.json').read_text()),protocol,manifest['cases'][0])
    if manifest['id']!=packets['manifest_id']:raise ValueError('pair source mismatch')
    with tempfile.TemporaryDirectory(prefix='shoal-pair-verify-') as tmp:
        rebuilt=prepare(repo,run/'source-manifest.json',model,extractor,protocol,Path(tmp)/'pair',
                        json.loads((run/'pr-metadata.json').read_text()))
        if rebuilt['status']!='ready' or rebuilt['packets']!=packets['packets']:
            raise ValueError('pair differs from frozen-model/source reconstruction')
    # Fail before paying either arm if input changed.
    for p in packets['packets']:
        raw=(run/f"pr-{p['pr']}-{p['arm']}.txt").read_bytes()
        if digest(raw)!=p['prompt_sha256'] or len(raw)!=p['prompt_bytes']:raise ValueError('pair prompt changed')
    for packet in sorted(packets['packets'],key=lambda p:digest([protocol['id'],p['pr'],p['arm']])):
        stem=f"pr-{packet['pr']}-{packet['arm']}";call_id='v11-'+stem;dest=run/'calls'/stem
        subprocess.run([sys.executable,str(Path(paid_review.__file__)),'--protocol',str(protocol_path),'--db',str(budget),
            '--prompt',str(run/(stem+'.txt')),'--call-id',call_id,'--claude',str(claude),'--output',str(dest)],check=True)
        for suffix in ('call.json','response.json','stderr.txt'):
            with (run/(stem+'.'+suffix)).open('xb') as out:out.write((dest/suffix).read_bytes())
    report=paired_report.summarize(run);report['protocol_id']=protocol['id'];report['status']='awaiting_source_adjudication';report['id']=digest(report);write_new(run/'report.json',report)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('command',choices=['carry-budget','prepare','run'])
    p.add_argument('--protocol',type=Path,required=True)
    for name in ('parent','budget','repo','manifest','model','extractor','output','run','claude','pr-metadata'):p.add_argument('--'+name,type=Path)
    p.add_argument('--rehearsal',action='store_true')
    a=p.parse_args();protocol=checked(a.protocol)
    required={'carry-budget':['parent','budget'],'prepare':['repo','manifest','model','extractor','output'],'run':['run','budget','claude','repo','model','extractor']}[a.command]
    for name in required:
        if getattr(a,name) is None:p.error('--'+name+' required')
    if a.command=='carry-budget':continue_budget(a.parent,a.budget,protocol)
    elif a.command=='prepare':prepare(a.repo,a.manifest,a.model,a.extractor,protocol,a.output,json.loads(a.pr_metadata.read_text()) if a.pr_metadata else None,a.rehearsal)
    else:run_pair(a.run,a.protocol,a.budget,a.claude,a.repo,a.model,a.extractor)


if __name__=='__main__':main()
