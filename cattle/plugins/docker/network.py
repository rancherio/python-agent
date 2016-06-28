import logging
import re

from cattle.plugins.docker.util import add_to_env, add_label, \
    is_nonrancher_container
from cattle.utils import get_or_create_map, get_or_create_list, \
    check_output

log = logging.getLogger('docker')


def setup_network_mode(instance, compute, client, create_config, start_config):
    """
    Based on the network configuration we choose the network mode to set in
    Docker.  We only really look for none, host, or container.  For all
    all other configurations we assume bridge mode
    """
    ports_supported = True
    hostname_support = True
    try:
        kind = instance.nics[0].network.kind
        if kind == 'dockerHost':
            ports_supported = False
            hostname_support = False
            start_config['network_mode'] = 'host'
            del start_config['links']
        elif kind == 'dockerNone':
            ports_supported = False
            create_config['network_disabled'] = True
            start_config['network_mode'] = 'none'
            del start_config['links']
        elif kind == 'dockerContainer':
            ports_supported = False
            hostname_support = False
            id = instance.networkContainer.uuid
            other = compute.get_container(client, instance.networkContainer)
            if other is not None:
                id = other['Id']
            start_config['network_mode'] = 'container:{}'.format(id)
            del start_config['links']
    except (KeyError, AttributeError, IndexError):
        pass

    return ports_supported, hostname_support


def setup_mac_and_ip(instance, create_config, set_mac=True, set_hostname=True):
    """
    Configures the mac address and primary ip address for the the supplied
    container. The mac_address is configured directly as part of the native
    docker API. The primary IP address is set as an environment variable on the
    container. Another Rancher micro-service will detect this environment
    variable when the container is started and inject the IP into the
    container.

    Note: while an instance can technically have more than one nic based on the
    resource schema, this implementation assumes a single nic for the purpose
    of configuring the mac address and IP.
    """
    mac_address = None
    device_number = None
    for nic in instance.nics:
        if device_number is None:
            mac_address = nic.macAddress
            device_number = nic.deviceNumber
        elif device_number > nic.deviceNumber:
            mac_address = nic.macAddress
            device_number = nic.deviceNumber

    if set_mac:
        create_config["mac_address"] = mac_address

    if not set_hostname:
        del create_config['hostname']

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
                add_label(create_config,
                          {'io.rancher.container.ip': ip_address})
    except (KeyError, AttributeError):
        pass


def setup_ports(instance, create_config, start_config, ports_supported=True):
    """
    Docker 1.9+ does not allow you to pass port info for networks that don't
    support ports (net, none, container:x)
    """
    if not ports_supported:
        start_config['publish_all_ports'] = False
        try:
            del create_config['ports']
        except:
            pass
        try:
            del start_config['port_bindings']
        except:
            pass


def _find_ip_and_mac(instance):
    for nic in instance.nics:
        for ip in nic.ipAddresses:
            if ip.role != 'primary':
                continue
            subnet = '{}/{}'.format(ip.subnet.networkAddress,
                                    ip.subnet.cidrSize)
            return ip.address, nic.macAddress, subnet
    return None, None, None


def setup_dns(instance):
    if not _has_service(instance, 'dnsService') or \
            instance.kind == 'virtualMachine':
        return

    ip_address, mac_address, subnet = _find_ip_and_mac(instance)

    if ip_address is None or mac_address is None:
        return

    try:
        parts = ip_address.split('.')
        if len(parts) != 4:
            return

        mark = str(int(parts[2]) * 1000 + int(parts[3]))

        check_output(['iptables', '-w', '-t', 'nat', '-A', 'CATTLE_PREROUTING',
                      '!', '-s', subnet, '-d', '169.254.169.250', '-m', 'mac',
                      '--mac-source', mac_address, '-j', 'MARK', '--set-mark',
                      mark])
        check_output(['iptables', '-w', '-t', 'nat', '-A',
                      'CATTLE_POSTROUTING', '!', '-s', subnet, '-d',
                      '169.254.169.250', '-m', 'mark', '--mark', mark, '-j',
                      'SNAT', '--to', ip_address])
    except:
        # ignore error
        pass


def setup_links(instance, create_config, start_config):
    """
    Sets up a container's config for rancher-managed links by removing the
    docker native link configuration and emulating links through environment
    variables.

    Note that a non-rancher container (one created and started outside the
    rancher API) container will not have its link configuration manipulated.
    This is because on a container restart, we would not be able to properly
    rebuild the link configuration because it depends on manipulating the
    create_config.
    """
    if not _has_service(instance, 'linkService') or is_nonrancher_container(
            instance):
        return

    if 'links' in start_config:
        del start_config['links']

    result = {}
    for link in instance.instanceLinks:
        link_name = link.linkName
        _add_link_env(link_name, link, result)
        _copy_link_env(link_name, link, result)

        try:
            for name in link.data.fields.instanceNames:
                _add_link_env(name, link, result, in_ip=link_name)
                _copy_link_env(name, link, result)

                # This does assume the format {env}_{name}
                parts = name.split('_', 1)
                if len(parts) == 1:
                    continue

                _add_link_env(parts[1], link, result, in_ip=link_name)
                _copy_link_env(parts[1], link, result)
        except AttributeError:
            pass

    if len(result) > 0:
        add_to_env(create_config, **result)


def _copy_link_env(name, link, result):
    try:
        targetInstance = link.targetInstance
        if targetInstance.data.dockerInspect.Config.Env is None:
            return

        ignores = set()

        for env in targetInstance.data.dockerInspect.Config.Env:
            parts = env.split('=', 1)
            if len(parts) == 1:
                continue

            if parts[1].startswith('/cattle/'):
                env_name = to_env_name(parts[1][len('/cattle/'):])
                ignores.add(env_name + '_NAME')
                ignores.add(env_name + '_PORT')
                ignores.add(env_name + '_ENV')

        for env in targetInstance.data.dockerInspect.Config.Env:
            should_ignore = False
            for ignore in ignores:
                if env.startswith(ignore):
                    should_ignore = True
                    break

            if should_ignore:
                continue

            parts = env.split('=', 1)
            if len(parts) == 1:
                continue
            key, value = parts[0], parts[1]
            if key in ['HOME', 'PATH']:
                continue

            result['{}_ENV_{}'.format(to_env_name(name), key)] = value
    except AttributeError:
        pass


def to_env_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).upper()


def _add_link_env(name, link, result, in_ip=None):
    try:
        result['{0}_NAME'.format(to_env_name(name)).upper()] =\
            '/cattle/{0}'.format(name)

        if link.data.fields.ports is None:
            return

        for link_port in link.data.fields.ports:
            proto = link_port.protocol
            ip = name.lower()
            if in_ip is not None:
                ip = in_ip
            dst = link_port.privatePort
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
                result['{0}_{1}'.format(to_env_name(name), k).upper()] = v
    except AttributeError:
        pass


def setup_ipsec(instance, host, create_config, start_config):
    """
    If the supplied instance is a network agent, configures the ports needed
    to achieve multi-host networking.
    """
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

        ports = get_or_create_list(create_config, 'ports')
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
