"""Metricbeat tabulated aggregation query facility.

Example of querying filesystem utilization and free bytes/inodes::

    from elasticsearch import Elasticsearch
    from offkey.metrics import build_bucket_search, build_result_buckets
    from pprint import pprint

    es = Elasticsearch(port=19201, scheme='https', verify_certs=False)
    ['.used.pct:max',
     '.free:min',
     '.free_files:min']
    axes = ['cloud.instance.id', '.mount_point']
    s = build_bucket_search(
        ['.used.pct:max', '.free:min', '.free_files:min'],
        module='system', metricset='filesystem',
        axes=['cloud.instance.id', '.mount_point'])
    r = s.using(es).execute()
    pprint(r.to_dict())  # print Elasticsearch query, in Python form.
    t = build_result_buckets(s.using(es).execute(), metrics,
                             module='system', metricset='filesystem',
                             axes=axes)
    pprint(t)  # print tabulated result

Example result from the above::

    {'i-00b05b0ec0795d08d': {'/': {'.free:min': 7054245888.0,
                                   '.free_files:min': 4154413.0,
                                   '.used.pct:max': 0.178}},
     'i-00cef31d24fb49a27': {'/': {'.free:min': 7078440960.0,
                                   '.free_files:min': 4154415.0,
                                   '.used.pct:max': 0.17500000000000002}},
     'i-00ddbddbe9134c9e3': {'/': {'.free:min': 7096557568.0,
                                   '.free_files:min': 4154416.0,
                                   '.used.pct:max': 0.17300000000000001}},
     'i-02d11a8c7b656af29': {'/': {'.free:min': 7096700928.0,
                                   '.free_files:min': 4154416.0,
                                   '.used.pct:max': 0.17300000000000001}},
     'i-040d9a77c92eddfd4': {'/': {'.free:min': 6956879872.0,
                                   '.free_files:min': 4154409.0,
                                   '.used.pct:max': 0.189}},
     'i-041ff6caee1975e2d': {'/': {'.free:min': 7054585856.0,
                                   '.free_files:min': 4154413.0,
                                   '.used.pct:max': 0.178}},
     'i-04671d193d2beae0d': {'/': {'.free:min': 7096389632.0,
                                   '.free_files:min': 4154416.0,
                                   '.used.pct:max': 0.17300000000000001}},
     'i-048e18bd42bfb3374': {'/': {'.free:min': 7061102592.0,
                                   '.free_files:min': 4154414.0,
                                   '.used.pct:max': 0.177}},
     'i-04d6a23d056891238': {'/': {'.free:min': 7038185472.0,
                                   '.free_files:min': 4154412.0,
                                   '.used.pct:max': 0.179}},
     'i-068038b1a51ff2c0a': {'/': {'.free:min': 7085850624.0,
                                   '.free_files:min': 4154416.0,
                                   '.used.pct:max': 0.17400000000000002}}}
"""

import math
import time
from typing import Sequence, Mapping, Union, Optional

from elasticsearch_dsl import Search

_mem_used_avg = Search(index='metricbeat-*') \
    .filter('term', **{'metricset.module': 'system'}) \
    .filter('term', **{'metricset.name': 'memory'}) \
    .filter('range', **{'@timestamp': {'gte': '2019-03-02T02:00:00Z',
                                       'lt':  '2019-03-02T02:05:00Z'}}) \
    .aggs \
    .bucket('host', 'terms', field='host.name') \
    .bucket('avg', 'avg', field='system.memory.used.pct')


def unix2es(ts: Union[float, int]) -> str:
    """Convert a UNIX timestamp to Elasticsearch-style ISO 8601 timestamp.

    The UTC time zone suffix (``Z``) is appended to the timestamp.

    Fractional second is rounded down to millisecond precision:

    >>> unix2es(3.14159)
    '1970-01-01T00:00:03.141Z'

    Whole seconds (`float` or `int`) get zeroed-out fractional part:

    >>> unix2es(3.0)
    '1970-01-01T00:00:03.000Z'
    >>> unix2es(3)
    '1970-01-01T00:00:03.000Z'

    :param ts: UNIX-style timestamp
    :return: a string in ISO 8601 UTC format with millisecond precision.
    """
    ymdhms = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(ts))
    ms = int((ts - math.floor(ts)) * 1000)
    return f'{ymdhms}.{ms:03d}Z'


DEFAULT_AXES = ('host.name',)
"""Default axes (bucket keys) to use if not specified."""

DEFAULT_AGG = 'avg'
"""Default aggregation to use for metrics specified without one."""

DEFAULT_WINDOW = 300.0
"""Default time window over which to aggregate."""


def build_bucket_search(metrics: Sequence[str],
                        metricset: Optional[str] = None,
                        module: Optional[str] = None,
                        axes: Sequence[str] = DEFAULT_AXES,
                        with_docs: bool = False,
                        window: Optional[float] = None,
                        end_time: Optional[float] = None) -> Search:
    """Build Elasticsearch bucket search for one or more Metricbeat metrics.

    Each metric specification in *metrics* takes the form of ``name[:agg]``.

    ``name`` is the metric name, such as ``system.memory.used.pct``.  ``agg``
    is the bucket aggregation function to use for the metric.

    *metricset* and *module* are the Metricbeat module and metricset names.
    The given *metrics* must all belong to this metricset; those that do
    not are not returned.

    If *metricset* and *module* are not specified, they are taken from the
    first two dotted components of the first metric name, a convention used by
    Metricbeat modules.  For example, ``system.memory.used.pct`` is assumed to
    be in the metricset ``memory`` provided by the ``system`` module.  This is
    useful when querying a single metric in the metricset.

    Conversely, if *metricset* and *module* are specified, metric names can be
    shortened by not having them and starting with a dot instead.  For
    example, if *module*=``system`` and *metricset*=``memory``, the metric
    name ``system.memory.used.pct`` can be shortened as ``.used.pct``.  This
    is useful when querying more than one metrics in the same metricset.

    *axes* are a sequence of aggregation bucket keys.  The default list
    includes only the ``host.name`` standard Metricbeat hostname.  It can be
    extended with more axes, ex: ``('host.name', 'system.network.name')`` for
    network interface table (the `network` metricset).  The host name axis
    can also be replaced by another axis, such as `cloud.instance.id`.
    Axis names can be shortened if the metricset and module names are known
    (given or deduced from *metrics*).

    *window* and *end_time* define the time range, and default to 5
    minutes (300 seconds) and the current UNIX time (on this system, not on
    Elasticsearch), respectively.

    By default, the build query requests Elasticsearch not to return matched
    documents.  *with_docs*=`True` changes this, and requests matched docs.

    :param metrics: Sequence of one or more metric specifications.
    :param metricset: Metricset name, such as `memory`.
    :param module: Module name, such as `system`.
    :param axes: Sequence of axis along which to split the result.
    :param with_docs: Whether to include raw documents matched.
    :param window: Duration over which to aggregate.
    :param end_time: The end time of the aggregation window.
    :return: Elasticsearch DSL query built.
    """
    metric_path = metrics[0].split(':')[0].split('.')
    if module is None:
        module = metric_path[0]
    if metricset is None:
        metricset = metric_path[1]
    if window is None:
        window = DEFAULT_WINDOW
    if end_time is None:
        end_time = time.time()

    def full(name):
        if name.startswith('.'):
            return module + '.' + metricset + name
        return name

    start_time = end_time - window
    s = Search(index='metricbeat-*') \
        .filter('term', **{'metricset.module': module}) \
        .filter('term', **{'metricset.name': metricset}) \
        .filter('range', **{'@timestamp': {'gte': unix2es(start_time),
                                           'lt':  unix2es(end_time)}})
    a = s.aggs
    for axis in axes:
        a = a.bucket(axis, 'terms', field=full(axis))
    for metric_agg in metrics:
        try:
            metric, agg = metric_agg.rsplit(':', 1)
        except ValueError:
            metric, agg = metric_agg, DEFAULT_AGG
        a.bucket(metric_agg, agg, field=full(metric))
    if not with_docs:
        s = s[:0]
    return s


def build_result_buckets(result, metrics: Sequence[str],
                         axes: Sequence[str] = DEFAULT_AXES) -> Mapping:
    """Tabulate execution result of a query built by `build_bucket_search()`.

    :param result: The return value of a `Search.execute()` call.
    :param metrics: See `build_bucket_search()`.
    :param axes: See `build_bucket_search()`.
    :return:
        The tabulated result.  It is a nested dict, with same depth as the
        number of axes, with the leaf dict keyed by metric specs.
    """
    def _brb(aggs: Mapping, remaining_axes: Sequence[str]):
        if not remaining_axes:
            return {metric: aggs[metric].value for metric in metrics}
        axis, remaining_axes = remaining_axes[0], remaining_axes[1:]
        return {
            bucket.key: _brb(bucket, remaining_axes)
            for bucket in aggs[axis]
        }

    return _brb(result.aggs, axes)
