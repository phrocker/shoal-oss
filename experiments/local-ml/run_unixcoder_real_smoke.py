#!/usr/bin/env python3
"""Run the pinned local UniXcoder challenger smoke without network access."""
import argparse
import hashlib
import json
import platform
import time
from pathlib import Path

import challenger_manifest as contract
import unixcoder_challenger as challenger


REPO = 'microsoft/unixcoder-base'
REVISION = '5604afdc964f6c53782a6813140ade5216b99006'
ROWS = (
    ('rename', 0, 'func RenameHelper(x int) int { return x }'),
    ('tenant-write', 1, 'func Update(ctx context.Context, tenant string, id string) error { return store.Put(ctx, id) }'),
    ('read', 0, 'func Read(ctx context.Context, id string) (Item,error) { return store.Get(ctx,id) }'),
    ('replay', 1, 'func Pay(ctx context.Context, charge Charge) error { return charge.Execute(ctx); return charge.Execute(ctx) }'),
    ('close', 0, 'func Close() error { return nil }'),
    ('grant', 1, 'func Grant(ctx context.Context, caller Principal, target Resource) error { return auth.Check(caller,target) }'),
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def manifest(snapshot):
    weights_path = snapshot / 'model.safetensors'
    if not weights_path.exists():
        weights_path = snapshot / 'pytorch_model.bin'
    tokenizer_digest = hashlib.sha256(
        (snapshot / 'vocab.json').read_bytes() + (snapshot / 'merges.txt').read_bytes()
    ).hexdigest()
    catboost = {
        'name': 'catboost-structured-v1', 'runtime_id': 'runtime-unixcoder-smoke',
        'weights_digest': 'a' * 64, 'tokenizer_digest': 'b' * 64, 'license_id': 'mit',
        'library_version': '1.2.8', 'categorical_features': ['operation_kind'],
    }
    unixcoder = {
        'name': 'unixcoder-linear-v1', 'runtime_id': 'runtime-unixcoder-smoke',
        'weights_digest': digest(weights_path), 'tokenizer_digest': tokenizer_digest,
        'license_id': 'mit', 'input_field': 'code', 'model_repo': REPO,
        'model_revision': REVISION, 'tokenizer_revision': REVISION,
        'max_context_tokens': 512, 'chunking': 'reject',
    }
    value = contract.build_manifest(
        'task-threat-v1', 'dataset-unixcoder-smoke-v1', 'split-unixcoder-smoke-v1',
        'features-code-v1', ['code'], 'labels-threat-v1', '2026-10-09T00:00:00Z',
        ['recall_at_budget'], {'max_trials': 1}, [catboost, unixcoder],
    )
    return value, weights_path, tokenizer_digest


def run(snapshot, device):
    value, weights_path, tokenizer_digest = manifest(snapshot)
    model = challenger.FrozenUnixCoder(value, device=device)
    train = [{'id': row_id, 'split': 'train', 'label': label, 'code': code} for row_id, label, code in ROWS]
    test = [dict(row, id='test-' + row['id'], split='test') for row in train]
    started = time.monotonic()
    artifact = challenger.train(train, value, model)
    result = challenger.predict(test, value, artifact, model)
    elapsed = time.monotonic() - started
    import torch
    import transformers
    return {
        'schema': 1, 'kind': 'unixcoder_real_smoke', 'repo': REPO, 'revision': REVISION,
        'snapshot_revision': REVISION, 'weights_sha256': digest(weights_path),
        'tokenizer_sha256': tokenizer_digest, 'manifest_id': value['id'],
        'artifact_id': artifact['id'], 'device': device, 'python': platform.python_version(),
        'torch': torch.__version__, 'transformers': transformers.__version__,
        'embedding_dimension': artifact['embedding_dimension'], 'training_rows': artifact['training_row_ids'],
        'scores': result['scores'], 'elapsed_seconds': elapsed,
        'optimization_enabled': result['optimization_enabled'],
        'label_authority': 'synthetic smoke labels; no quality claim',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--device', choices=('cpu',), default='cpu')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not args.snapshot.is_dir() or args.snapshot.name != REVISION:
        raise SystemExit('snapshot must be the pinned UniXcoder revision directory')
    report = run(args.snapshot, args.device)
    with args.output.open('x') as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps(report, sort_keys=True))


if __name__ == '__main__':
    main()
