import logging

from ..compute import DockerCompute
from ..util import add_label
from cattle.agent.handler import BaseHandler

log = logging.getLogger('docker')


class NetworkSetup(BaseHandler):
    def __init__(self):
        self.compute = DockerCompute()
        pass

    def before_start(self, instance, host, config, start_config):
        mac_address = None
        device_number = None
        for nic in instance.nics:
            if device_number is None:
                mac_address = nic.macAddress
                device_number = nic.deviceNumber
            elif device_number > nic.deviceNumber:
                mac_address = nic.macAddress
                device_number = nic.deviceNumber
        config["mac_address"] = mac_address

        try:
            if instance.nics and instance.nics[0].ipAddresses:
                # Assume one nic
                nic = instance.nics[0]
                ip_address = None
                for ip in nic.ipAddresses:
                    if ip.role == 'primary':
                        ip_address = '{0}/{1}'.format(ip.address,
                                                      ip.subnet.cidrSize)
                        break

                if ip_address:
                    add_label(config, RANCHER_IP=ip_address)
        except (KeyError, AttributeError):
            pass

    def after_start(self, instance, host, id):
        pass
