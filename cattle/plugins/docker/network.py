import logging

from cattle.plugins.docker.util import add_to_env, add_label
from cattle.utils import get_or_create_map, get_or_create_list

log = logging.getLogger('docker')


def setup_mac_and_ip(instance, create_config):
    mac_address = None
    device_number = None
    for nic in instance.nics:
        if device_number is None:
            mac_address = nic.macAddress
            device_number = nic.deviceNumber
        elif device_number > nic.deviceNumber:
            mac_address = nic.macAddress
            device_number = nic.deviceNumber
    create_config["mac_address"] = mac_address

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
                add_label(create_config, RANCHER_IP=ip_address)
    except (KeyError, AttributeError):
        pass


def setup_ports(instance, create_config, start_config):
    if not _has_service(instance, 'portService'):
        return

    if 'ports' in create_config:
        del create_config['ports']

    start_config['publish_all_ports'] = False


def setup_links(instance, create_config, start_config):
    if not _has_service(instance, 'linkService'):
        return

    if 'links' in start_config:
        del start_config['links']

    result = {}
    for link in instance.instanceLinks:
        name = link.linkName

        try:
            for link_port in link.data.fields.ports:
                proto = link_port.protocol
                ip = link_port.ipAddress
                dst = link_port.publicPort
                port = link_port.privatePort

                full_port = '{0}://{1}:{2}'.format(proto, ip, dst)

                data = {
                    'NAME': '/cattle/{0}'.format(name),
                    'PORT': full_port,
                    'PORT_{0}_{1}'.format(port, proto): full_port,
                    'PORT_{0}_{1}_ADDR'.format(port, proto): ip,
                    'PORT_{0}_{1}_PORT'.format(port, proto): dst,
                    'PORT_{0}_{1}_PROTO'.format(port, proto): proto,
                }

                for k, v in data.items():
                    result['{0}_{1}'.format(name, k).upper()] = v
        except AttributeError:
            pass

    if len(result) > 0:
        add_to_env(create_config, **result)


def setup_ipsec(instance, host, config, start_config):
    if instance.get('agentId') is None:
        network_agent = False
    elif instance.get('systemContainer') is None or \
            instance.get('systemContainer') == 'NetworkAgent':
        network_agent = True
    else:
        network_agent = False

    if not network_agent or \
            not _has_service(instance, 'ipsecTunnelService'):
        return

    try:
        host_id = str(host.id)
        nat = instance.data.ipsec[host_id]['nat']
        isakmp = instance.data.ipsec[host_id]['isakmp']

        ports = get_or_create_list(config, 'ports')
        binding = get_or_create_map(start_config, 'port_bindings')

        ports.append((500, 'udp'))
        ports.append((4500, 'udp'))
        binding['500/udp'] = ('0.0.0.0', isakmp)
        binding['4500/udp'] = ('0.0.0.0', nat)
    except (KeyError, AttributeError):
        pass




def _has_service(instance, kind):
    try:
        for nic in instance.nics:
            if nic.deviceNumber != 0:
                continue

            for service in nic.network.networkServices:
                if service.kind == kind:
                    return True
    except (KeyError, AttributeError):
        pass

    return False
