import platform
import datetime
from cattle.utils import CadvisorAPIClient
from cattle import Config


class NetworkCollector(object):
    def __init__(self):
        self.cadvisor = CadvisorAPIClient(Config.cadvisor_ip(),
                                          Config.cadvisor_port())

    def key_name(self):
        return "networkInfo"

    def get_data(self):
        data = {}

        if platform.system() == 'Linux':
            data.update(self._get_linux_network_info())

        return data

    def _get_linux_network_info(self):
        stats = self.cadvisor.get_stats()
        data = {}
        start = stats[0]
        end = stats[len(stats) - 1]

        time_fmt = "%Y-%m-%dT%H:%M:%S"
        end_time = end["timestamp"][0:19]
        start_time = start["timestamp"][0:19]

        diff = datetime.datetime.strptime(end_time, time_fmt) \
            - datetime.datetime.strptime(start_time, time_fmt)
        time_elapsed = divmod(diff.days *86400 + diff.seconds, 60)
        time_elapsed = 60 * time_elapsed[0] + time_elapsed[1]

        data['rx_bytes'] = end["network"]["rx_bytes"]\
                               - start["network"]["rx_bytes"]
        data['tx_bytes'] = end["network"]["tx_bytes"]\
                               - start["network"]["tx_bytes"]

        data['rx_bytes_sec'] = data['rx_bytes'] / time_elapsed
        data['tx_bytes_sec'] = data['tx_bytes'] / time_elapsed

        data['start_time'] = start['timestamp']
        data['end_time'] = end['timestamp']

        return data
