import os
import dill as pickle
from io import BytesIO
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    BinaryType,
)

from ...element import Element
from ..dm_collector import (
    dm_collector_c,
    DMLogPacket,
    FormatError,
)


class SparkDecoder(Element):
    '''Single file decoder

    Do not use this class directly
    '''

    def __init__(self, work_dir, name, output_dir, sampling_rate,
                 type_names, skip_decoding):
        os.chdir(work_dir)
        Element.__init__(self)
        DMLogPacket.init({})

        # set_sampling_rate() if called in parent
        if (sampling_rate > 0):
            dm_collector_c.set_sampling_rate(sampling_rate)

        # enable_log()
        self._type_names = type_names
        dm_collector_c.set_filtered(self._type_names)

        # set_skip_decoding() if called in parent
        self._skip_decoding = skip_decoding

        # save_log_as() if called in parent
        if output_dir:
            output_name = name + '.mi2log' if len(name.split(
                '.')) < 2 else '.'.join(name.split('.')[:-1] + ['mi2log'])
            dm_collector_c.set_filtered_export(
                os.path.join(output_dir, output_name), self._type_names)

    def decode(self, input_file):
        packets = []
        dm_collector_c.reset()
        data = BytesIO(input_file.content)
        while True:
            s = data.read(64)

            if s:
                dm_collector_c.feed_binary(s)

            decoded = dm_collector_c.receive_log_packet(self._skip_decoding,
                                                        True)  # include TS
            if not s and not decoded:
                # EOF encountered and no message can be received any more
                break

            if decoded:
                try:
                    if not decoded[0]:
                        continue

                    packet = DMLogPacket(decoded[0])
                    type_id = packet.get_type_id()

                    if (type_id in self._type_names
                            or type_id == "Custom_Packet"):
                        # Based on the current implementation, the DMLogPacket
                        # should be safe to pickle.
                        #
                        # For extra safety, use the _decoded_list argument.
                        packets.append((packet.decode()['timestamp'],
                                        type_id,
                                        pickle.dumps(decoded[0])))

                except FormatError as e:
                    # skip this packet
                    print(("FormatError: ", e))

        return (input_file.path,
                input_file.modificationTime,
                len(packets),
                packets)
