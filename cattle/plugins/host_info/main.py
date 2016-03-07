import logging

from cattle.plugins.host_info.memory import MemoryCollector
from cattle.plugins.host_info.os_c import OSCollector
from cattle.plugins.host_info.cpu import CpuCollector
from cattle.plugins.host_info.disk import DiskCollector
from cattle.plugins.host_info.network import NetworkCollector

log = logging.getLogger('host_info')


class HostInfo(object):
    def __init__(self, docker_client=None):
        self.docker_client = docker_client

        self.collectors = [MemoryCollector(),
                           OSCollector(self.docker_client),
                           DiskCollector(self.docker_client),
                           CpuCollector(),
                           NetworkCollector()]

    def collect_data(self):
        data = {}
        for collector in self.collectors:
            try:
                data[collector.key_name()] = collector.get_data()
            except:
                log.exception(
                    "Error collecting {0} stats".format(collector.key_name()))
                data[collector.key_name()] = {}

        return data

    def host_labels(self, label_pfx="io.rancher.host"):
        labels = {}
        for collector in self.collectors:
            try:
                get_labels = getattr(collector, "get_labels", None)
                if callable(get_labels):
                    labels.update(get_labels(label_pfx))
            except:
                log.exception(
                    "Error getting {0} labels".format(collector.key_name()))

        return labels if len(labels) > 0 else None
