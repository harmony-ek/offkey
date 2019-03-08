# -*- coding: utf-8 -*-

"""Console script for offkey."""

from base64 import b64encode
from collections import OrderedDict
from hashlib import sha256
from itertools import chain
import json
import sys
import time
import hashlib

import click
from elasticsearch import Elasticsearch
import requests

from .metrics import build_bucket_search, build_result_buckets
from .util import flatten

HOST_AXES = ('cloud.region', 'cloud.instance.id',)

CONFIGS = [
    dict(
        module='system', metricset='cpu',
        metrics={
            '.total.pct:avg': dict(
                info=90, warning=95, error=97, critical=99,
                message='High CPU percentage (1-minute average)',
            )
        },
        window=60, period=10,
    ),
    dict(
        module='system', metricset='cpu',
        metrics={
            '.total.pct:avg': dict(
                info=80, warning=90, error=95, critical=98,
                message='High CPU percentage (5-minute average)',
            )
        },
        window=300, period=60,
    ),
    dict(
        module='system', metricset='cpu',
        metrics={
            '.total.pct:avg': dict(
                info=70, warning=80, error=90, critical=95,
                message='High CPU percentage (15-minute average)',
            )
        },
        window=900, period=60,
    ),
    dict(
        module='system', metricset='load',
        metrics={
            '.norm.1:max': dict(
                info=10, warning=20, error=30, critical=40,
                message='High 1-minute CPU load',
            ),
            '.norm.5:max': dict(
                info=5, warning=10, error=15, critical=20,
                message='High 5-minute CPU load',
            ),
            '.norm.15:max': dict(
                info=2.5, warning=5, error=7.5, critical=10,
                message='High 15-minute CPU load',
            ),
        },
        window=60, period=30,
    ),
    dict(
        module='system', metricset='memory',
        metrics={
            '.used.pct:avg': dict(
                info=80, warning=90, error=95, critical=98,
                message='High memory use',
            )
        },
        window=60, period=30,
    ),
    dict(
        module='system', metricset='diskio', axes=['.name'],
        metrics={
            '.iostat.busy': dict(
                info=90, warning=95, error=97, critical=99,
                message='High 1-minute disk I/O',
            )
        },
        window=60, period=60,
    ),
    dict(
        module='system', metricset='diskio', axes=['.name'],
        metrics={
            '.iostat.busy': dict(
                info=80, warning=90, error=95, critical=97,
                message='High 5-minute disk I/O',
            )
        },
        window=300, period=60,
    ),
    dict(
        module='system', metricset='diskio', axes=['.name'],
        metrics={
            '.iostat.busy': dict(
                info=80, warning=90, error=95, critical=97,
                message='High 15-minute disk I/O',
            )
        },
        window=900, period=60,
    ),
    dict(
        module='system', metricset='process',
        axes=['.pid', '.cpu.start_time', '.name'],  # for PID recycling
        metrics={
            '.cpu.total.pct': dict(
                info=0.001, warning=0.002, error=0.005, critical=0.01,
                message='High process CPU usage',
            )
        },
        window=60, period=30,
    ),
]


# TODO - honor period
# TODO - add differential/rate support (network needs it)
# TODO - support class-based config (ex: diff. throughput per instance type)


def strictly_increasing(L):
    """Return whether the given sequence is strictly increasing."""
    return all(x < y for x, y in zip(L, L[1:]))


def strictly_decreasing(L):
    """Return whether the given sequence is strictly decreasing."""
    return all(x > y for x, y in zip(L, L[1:]))


def _check_val_against_thresh(val, thresh_config):
    tl = []
    for level in 'info', 'warning', 'error', 'critical':
        thresh = thresh_config.get(level)
        if thresh is not None:
            tl.append((level, thresh))
    if not tl:
        # No thresholds, nothing to do
        return None, None
    tvs = [t[1] for t in tl]
    si = strictly_increasing(tvs)
    sd = strictly_decreasing(tvs)
    assert si or sd, (
        f"{thresh_config!r} is neither strictly increasing nor "
        f"strictly decreasing"
    )
    # caveat emptor: si == sd == True if len(tl) == 1
    severity = None
    ref = None
    for level, thresh in tl:
        if (si and val < thresh) or (not si and val > thresh):
            break
        severity, ref = level, thresh
    if severity is None:
        return None, None
    if si:
        diag = f'{val} >= {ref}'
    else:
        diag = f'{val} <= {ref}'
    return severity, diag


@click.command()
def main(args=None):
    """Console script for offkey."""
    from pprint import pprint
    es = Elasticsearch(port=19201, scheme='https', verify_certs=False)
    # TODO ek – quick-and-dirty impl; use asyncio to parallelize
    for config in CONFIGS:
        module = config['module']
        metricset = config['metricset']
        def full(s):
            if s.startswith('.'):
                return f'{module}.{metricset}{s}'
            return s
        axes = tuple(config.get('axes', ()))
        full_axes = HOST_AXES + axes
        metric_configs = config['metrics']
        metrics = tuple(metric_configs.keys())
        window = config['window']
        # period = search_config['period']  # TODO use when looped
        q = build_bucket_search(metrics, axes=full_axes,
                                metricset=metricset, module=module,
                                window=window)
        r = q.using(es).execute()
        t = build_result_buckets(r, metrics=metrics, axes=full_axes)
        for k, v in flatten(t):
            assert len(k) >= len(HOST_AXES) + 1
            host_axes = k[:len(HOST_AXES)]
            metricset_axes = k[len(HOST_AXES):-1]
            metric_spec = k[-1]
            full_metric_spec = full(metric_spec)
            assert len(axes) == len(metricset_axes)
            metric_config = metric_configs[metric_spec]
            severity, diag = _check_val_against_thresh(v, metric_config)
            host_axes_items = zip(map(full, HOST_AXES), host_axes)
            metricset_axes_items = zip(map(full, axes), metricset_axes)
            all_items = sorted(chain(
                host_axes_items,
                metricset_axes_items,
                ((full_metric_spec, None),),
            ))
            key = b64encode(sha256(json.dumps(all_items).encode()).digest()).decode()
            body = dict(dedup_key=key)
            if severity is None:
                body.update(event_action='resolve')
            else:
                now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                custom_details = {}
                custom_details.update(host_axes_items)
                custom_details.update(metricset_axes_items)
                custom_details[full_metric_spec] = v
                source = '/'.join(str(axis) for axis in host_axes)
                component = '/'.join(str(axis) for axis in metricset_axes)
                message = metric_config.get('message', 'Metric out of range')
                payload = {
                    'summary': f'{message}: {full_metric_spec} {diag}',
                    'source': source,
                    'severity': severity,
                    'timestamp': now,
                    'component': component,
                    'class': message,
                    'custom_details': custom_details,
                }
                body.update(event_action='trigger', payload=payload)
            pprint(body)
            rk = 'ce942cb5245643edb20b6dc56a6e14f1'  # XXX fixthis
            res = requests.post('https://events.pagerduty.com/v2/enqueue',
                                json=body, headers={'X-Routing-Key': rk})
            try:
                res.raise_for_status()
            except Exception as e:
                print(res.content)
    return 0


if __name__ == "__main__":
    import logging; logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())  # pragma: no cover
