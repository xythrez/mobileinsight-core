# -*- coding: utf-8 -*-

from importlib.util import find_spec

__all__ = [
    "AndroidDevDiagMonitor",
    "Monitor",
    "DMCollector",  # P4A: THIS LINE WILL BE DELETED ###
    "OfflineReplayer",
    "OnlineMonitor",
    "AndroidMtkMonitor",
    "MtkOfflineReplayer"
]

is_android = False
try:
    from jnius import autoclass  # For Android
    is_android = True
except Exception as e:
    # not used, but bugs may exist on laptop
    is_android = False


if is_android:
    from .android_dev_diag_monitor import AndroidDevDiagMonitor
    from .android_mtk_monitor import AndroidMtkMonitor
from .monitor import Monitor
from .dm_collector import *  # P4A: THIS LINE WILL BE DELETED ###
from .offline_replayer import OfflineReplayer
from .online_monitor import OnlineMonitor
from .mtk_offline_replayer import MtkOfflineReplayer


def has_module(name):
    try:
        return find_spec(name) is not None
    except ModuleNotFoundError:
        return False


if (has_module('pyspark')
        and has_module('pandas')
        and has_module('pyarrow')
        and has_module('dill')):
    __all__ += [
        'SparkReplayer',
        'group_by',
        'collect_by',
    ]
    from .spark import group_by, collect_by
    from .spark.spark_replayer import SparkReplayer
