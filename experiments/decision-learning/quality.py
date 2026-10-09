#!/usr/bin/env python3
"""Bounded, provider-neutral quality reports for local decision challengers.

The evaluator measures a declared labeled cohort; it never turns a targeted or
unweighted sample into a population claim and never authorizes promotion.
"""
import hashlib
import json
import math


SCHEMA = 1


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def seal(kind, **fields):
    body = dict(schema=SCHEMA, kind=kind, **fields)
    return dict(body, id=_digest(body))


def verify(report):
    if not isinstance(report, dict) or report.get('schema') != SCHEMA or report.get('kind') != 'quality_report':
        raise ValueError('invalid quality report')
    body = {key: value for key, value in report.items() if key != 'id'}
    if report.get('id') != _digest(body):
        raise ValueError('quality report identity mismatch')
    if report.get('promotion_eligible') is not False:
        raise ValueError('quality report cannot authorize promotion')
    return report


def _finite(value, name, low=0.0, high=1.0):
    if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
        raise ValueError('invalid ' + name)


def _wilson(successes, total):
    if total == 0:
        return None
    z = 1.959963984540054
    p = successes / total
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return [max(0.0, center - spread), min(1.0, center + spread)]


def evaluate(labels, scores, groups, *, sample, threshold=0.5, bins=10, costs=None):
    if not isinstance(labels, dict) or not labels or len(labels) > 1_000_000 or set(labels) != set(scores) or set(labels) != set(groups):
        raise ValueError('labels, scores and groups must have identical nonempty IDs')
    if type(bins) is not int or not 2 <= bins <= 20:
        raise ValueError('invalid calibration bins')
    _finite(threshold, 'threshold')
    if not isinstance(sample, dict) or sample.get('split') != 'test' or sample.get('predeclared') is not True:
        raise ValueError('quality sample must be predeclared test data')
    kind = sample.get('kind')
    if kind not in ('uniform', 'random', 'targeted'):
        raise ValueError('invalid sample kind')
    if sample.get('future_inputs') is not False:
        raise ValueError('future inputs are not permitted')
    if type(sample.get('population')) is not int or not 0 < sample['population'] <= 1_000_000_000 or sample['population'] < len(labels):
        raise ValueError('invalid sample population')
    resolved = []
    unknown = disputed = 0
    for row_id, label in labels.items():
        score = scores[row_id]
        _finite(score, 'score')
        if (not isinstance(row_id, str) or not row_id or len(row_id) > 512
                or not isinstance(groups[row_id], str) or not groups[row_id] or len(groups[row_id]) > 512):
            raise ValueError('invalid row identity or family')
        if label in ('unknown', 'disputed'):
            unknown += label == 'unknown'; disputed += label == 'disputed'
            continue
        if type(label) is not int or label not in (0, 1):
            raise ValueError('invalid label')
        resolved.append((row_id, label, score))
    if not resolved:
        raise ValueError('no resolved labels')
    predicted = {row_id: int(score >= threshold) for row_id, _, score in resolved}
    positives = sum(label for _, label, _ in resolved)
    negatives = len(resolved) - positives
    tp = sum(predicted[row_id] == label == 1 for row_id, label, _ in resolved)
    tn = sum(predicted[row_id] == label == 0 for row_id, label, _ in resolved)
    fp = sum(predicted[row_id] == 1 and label == 0 for row_id, label, _ in resolved)
    fn = sum(predicted[row_id] == 0 and label == 1 for row_id, label, _ in resolved)
    recall = tp / positives if positives else None
    specificity = tn / negatives if negatives else None
    brier = sum((score - label) ** 2 for _, label, score in resolved) / len(resolved)
    calibration = []
    for index in range(bins):
        lower, upper = index / bins, (index + 1) / bins
        bucket = [(label, score) for _, label, score in resolved if lower <= score < upper or index == bins - 1 and score == upper]
        if bucket:
            calibration.append({'lower': lower, 'upper': upper, 'count': len(bucket),
                                'mean_score': sum(score for _, score in bucket) / len(bucket),
                                'positive_rate': sum(label for label, _ in bucket) / len(bucket)})
    ece = sum(row['count'] * abs(row['mean_score'] - row['positive_rate']) for row in calibration) / len(resolved)
    cost = {'inference_seconds': 0.0, 'training_seconds': 0.0, 'label_seconds': 0.0}
    if costs is not None:
        if not isinstance(costs, dict):
            raise ValueError('invalid costs')
        for key in cost:
            value = costs.get(key, 0.0)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError('invalid cost ' + key)
            cost[key] = float(value)
    return seal('quality_report', sample=sample, threshold=threshold,
                counts={'total': len(labels), 'resolved': len(resolved), 'unknown': unknown,
                        'disputed': disputed, 'positive': positives, 'negative': negatives,
                        'true_positive': tp, 'true_negative': tn, 'false_positive': fp, 'false_negative': fn},
                metrics={'accuracy': (tp + tn) / len(resolved), 'recall': recall,
                         'specificity': specificity, 'brier': brier, 'ece': ece,
                         'recall_wilson_95': _wilson(tp, positives)},
                calibration=calibration, families=len(set(groups.values())), costs=cost,
                population_claim=(kind in ('uniform', 'random') and sample['population'] == len(labels)
                                  and unknown == 0 and disputed == 0),
                promotion_eligible=False,
                limitation='Measurement only; unknown/disputed rows excluded, targeted samples are not population estimates.')
