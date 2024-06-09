import os
import time
import dill as pickle
from threading import Lock
from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    LongType,
    MapType,
    StringType,
    StructType,
    StructField,
    TimestampType,
)

from ..offline_replayer import OfflineReplayer
from .decoder import SparkDecoder
from .submonitor import SparkSubmonitor
from . import group_by, collect_by


def _collect(self):
    return self.source.spark_results[self]


def _identity(self):
    return self


class SparkReplayer(OfflineReplayer):
    '''Spark-backend OfflineReplayer

    This Replayer is only available if PySpark is installed
    '''

    _reflock = Lock()
    _refcnt = 0
    _spark = None

    def __init__(self):
        OfflineReplayer.__init__(self)

        # Keep a reference count of SparkReplayers
        #
        # Spark already provides a way to create sessions once using
        # getOrCreate(), but this allows us to terminate the session
        # once all references are lost.
        with SparkReplayer._reflock:
            if SparkReplayer._refcnt == 0:
                # Use the Spark URL in the MI_SPARK_URL variable,
                # fallback to pyspark's local session if unavailable
                mi_spark_url = os.getenv('MI_SPARK_URL', 'local[1]')
                # Only print Spark logs error or above by default
                mi_log_level = os.getenv('MI_SPARK_LOG_LEVEL', 'error')
                SparkReplayer._spark = (SparkSession.builder
                                        .master(mi_spark_url)
                                        .appName('mobile_insight')
                                        .getOrCreate())
                SparkReplayer._spark.sparkContext.setLogLevel(mi_log_level)
            SparkReplayer._refcnt += 1

        self._sampling_rate = -1
        self._output_path = None
        self._group_function = group_by.file_path
        self._analyzer_info = {}
        self.spark_results = {}

    def __del__(self):
        # Decrease reference on garbage collection
        # If refcnt drops to 0, stop the SparkSession
        with SparkReplayer._reflock:
            SparkReplayer._refcnt -= 1
            if SparkReplayer._refcnt == 0:
                try:
                    SparkReplayer._spark.stop()
                # For some reason stop() is async, which means it could be
                # called twice during python shutdown.
                # Simply ignore the error and move on if this happens.
                except ImportError:
                    pass

        OfflineReplayer.__del__(self)

    def set_sampling_rate(self, sampling_rate):
        OfflineReplayer.set_sampling_rate(self, sampling_rate)
        # Need to propagate this to SubMonitors
        self._sampling_rate = sampling_rate

    def save_log_as(self, path):
        # Do not call the OfflineReplayer version. It opens the file
        # immediately.
        # Instead, save the path and create a directory with the files
        # during execution.
        path = os.path.abspath(path)
        os.makedirs(path, exist_ok=True)
        self._output_path = path

    def set_group_function(self, func):
        '''Manually group the packet logs into tasks.

        Task is the minimum partioning unit SparkReplayer uses to dispatch
        logs to specific analyzers.
        '''
        self._group_function = func

    def register(self, analyzer, spark_init=None, spark_export=None,
                 spark_collect=None):
        '''Register an analyzer.

        This function has been overloaded to support registering special
        functions/args for SparkReplayer. These special functions/args must
        have the following properties:

        spark_init - Arguments to initialize an analyzer
          - Can be either a list of positional arguments used to initialize
            the analyzer "x = [a, b, c]" -> "MyAnalyzer(*x)"; or a function
            that returns a list of positional arguments.
          - If None is specified, the class method "MyAnalyzer.spark_init()"
            is used. Declare this with the "@classmethod" to avoid errors.
          - If the class method does not exist, a default value of "[]" is
            used.
        spark_export - Export results from workers
          - Must be a function of signiture "MyAnalyzer -> Any".
          - If None is specified, the class method
            "MyAnalyzer.spark_export(analyzer_obj)" is used. Declare this
            with the "@classmethod" to avoid errors.
          - If the class method does not exist, the identity function is used.
        spark_collect - Reduce exported results to a single value
          - Must be a function of signiture "list[Any] -> Any".
          - If None is specified, the class method
            "MyAnalyzer.spark_collect(analyzer_objs)" is used. Declare this
            with the "@classmethod" to avoid errors.
          - If the class method does not exist, the identity function is used.
        '''
        OfflineReplayer.register(self, analyzer)
        if spark_init is None:
            try:
                getattr(analyzer, 'spark_init')
                spark_init = analyzer.__class__.spark_init
            except AttributeError:
                spark_init = []
        if spark_collect is None:
            try:
                getattr(analyzer, 'spark_collect')
                spark_collect = analyzer.__class__.spark_collect
            except AttributeError:
                spark_collect = _identity
        if spark_export is None:
            try:
                getattr(analyzer, 'spark_export')
                spark_export = analyzer.__class__.spark_export
            except AttributeError:
                spark_export = _identity
        if analyzer not in self._analyzer_info:
            self._analyzer_info[analyzer] = (id(analyzer), analyzer.__class__,
                                             spark_init, spark_collect,
                                             spark_export)
            try:
                getattr(analyzer, 'collect')
            except AttributeError:
                self.log_info(('%s is a default analyzer, adding "collect()" '
                              'function for data collection')
                              % str(analyzer.__class__))
                analyzer.collect = _collect.__get__(analyzer,
                                                    analyzer.__class__)

    def set_analyzer_callbacks(self, analyzer, spark_init=None,
                               spark_export=None, spark_collect=None):
        '''Register an analyzer callback

        This should be called if one wants to change the SparkReplayer
        callback functions after registering the analyzer. These special
        functions/args must have the following properties:

        spark_init - Arguments to initialize an analyzer
          - Can be either a list of positional arguments used to initialize
            the analyzer "x = [a, b, c]" -> "MyAnalyzer(*x)"; or a function
            that returns a list of positional arguments.
          - If None is specified, the class method "MyAnalyzer.spark_init()"
            is used. Declare this with the "@classmethod" to avoid errors.
          - If the class method does not exist, a default value of "[]" is
            used.
        spark_export - Export results from workers
          - Must be a function of signiture "MyAnalyzer -> Any".
          - If None is specified, the class method
            "MyAnalyzer.spark_export(analyzer_obj)" is used. Declare this
            with the "@classmethod" to avoid errors.
          - If the class method does not exist, the identity function is used.
        spark_collect - Reduce exported results to a single value
          - Must be a function of signiture "list[Any] -> Any".
          - If None is specified, the class method
            "MyAnalyzer.spark_collect(analyzer_objs)" is used. Declare this
            with the "@classmethod" to avoid errors.
          - If the class method does not exist, the identity function is used.
        '''
        if analyzer in self._analyzer_info:
            curr = self._analyzer_info[analyzer]
            self._analyzer_info[analyzer] = (
                id(analyzer),
                analyzer.__class__,
                spark_init if spark_init is not None else curr[2],
                spark_collect if spark_collect is not None else curr[3],
                spark_export if spark_export is not None else curr[4]
            )
        else:
            self.log_info(('%s is not a registered analyzer. '
                           'Ignoring callback registration')
                          % str(analyzer.__class__))

    def deregister(self, analyzer):
        OfflineReplayer.deregister(self, analyzer)
        if analyzer in self._analyzer_info:
            self._analyzer_info.pop(analyzer)

    def run(self):
        decoded_schema = StructType([
            StructField('file_path', StringType(), False),
            StructField('file_mtime', TimestampType(), False),
            StructField('file_packets', LongType(), False),
            StructField('content', ArrayType(StructType([
                StructField('timestamp', TimestampType(), False),
                StructField('type_id', StringType(), False),
                StructField('packet', BinaryType(), False),
            ])), False),
        ])

        # Collect data from both qmdl and mi2logs
        logs = (SparkReplayer
                ._spark.read.format("binaryFile")
                .option("pathGlobFilter", "*.qmdl")
                .load(self._input_path)
                .union(SparkReplayer
                       ._spark.read.format("binaryFile")
                       .option("pathGlobFilter", "*.mi2log")
                       .load(self._input_path)))

        # Decode files
        before_decode_time = time.time()
        args = [
            os.getcwd(),
            None,
            self._output_path,
            self._sampling_rate,
            self._type_names,
            self._skip_decoding,
        ]
        decoded = (logs.rdd.map(lambda x:
                                SparkDecoder(args[0],
                                             os.path.basename(x.path),
                                             args[2],
                                             args[3],
                                             args[4],
                                             args[4]).decode(x))
                   .toDF(decoded_schema)
                   .select('*', posexplode('content'))
                   .drop('content')
                   .withColumnRenamed('pos', 'order')
                   .select('*', 'col.timestamp', 'col.type_id', 'col.packet')
                   .drop('col'))

        # Force eager evaluation to ensure side-effects occur (save-to-disk)
        decoded.cache().count()
        after_decode_time = time.time()

        # Partition the data, then launch submonitors and collect results
        args = [
            os.getcwd(),
            list(self._analyzer_info.values()),
        ]
        results = (self._group_function(decoded).applyInPandas(
            lambda x: SparkSubmonitor(*args).run(x),
            '_ int, obj map<long, binary>').drop('_'))

        for analyzer, tup in self._analyzer_info.items():
            analyzer_id, _, _, collect_func, _ = tup
            self.spark_results[analyzer] = collect_func([
                pickle.loads(x['result']) for x in results.select('obj.' + str(
                    analyzer_id)).toDF('result').collect()])
        after_sending_time = time.time()
        decoding_inter = after_decode_time - before_decode_time
        sending_inter = after_sending_time - after_decode_time
        self.log_info('decoding_inter_sum: ' + str(decoding_inter))
        self.log_info('sending_inter_sum: ' + str(sending_inter))
        self.log_info("Spark replay is completed.")
