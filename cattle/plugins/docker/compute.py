import logging
from os import path
import os

from . import docker_client, pull_image
from . import DockerConfig
from cattle import Config
from cattle.compute import BaseComputeDriver
from cattle.agent.handler import KindBasedMixin
from cattle.type_manager import get_type, MARSHALLER
from cattle import utils
from docker.errors import APIError
from cattle.plugins.host_info.main import HostInfo
from cattle.plugins.docker.util import add_label
from cattle.progress import Progress
from cattle.lock import lock
from cattle.plugins.docker.network import setup_ipsec, setup_links, \
    setup_mac_and_ip, setup_ports
from cattle.plugins.docker.agent import setup_cattle_config_url

log = logging.getLogger('docker')

# Docker-py doesn't support working_dir, maybe in 0.2.4?
CREATE_CONFIG_FIELDS = [
    ('environment', 'environment'),
    ('directory', 'working_dir'),
    ('user', 'user'),
    ('domainName', 'domainname'),
    ('memory', 'mem_limit'),
    ('memorySwap', 'memswap_limit'),
    ('cpuSet', 'cpuset'),
    ('cpuShares', 'cpu_shares'),
    ('tty', 'tty'),
    ('stdinOpen', 'stdin_open'),
    ('detach', 'detach'),
    ('entryPoint', 'entrypoint')]

START_CONFIG_FIELDS = [
    ('capAdd', 'cap_add'),
    ('capDrop', 'cap_drop'),
    ('dnsSearch', 'dns_search'),
    ('dns', 'dns'),
    ('publishAllPorts', 'publish_all_ports'),
    ('lxcConf', 'lxc_conf'),
    ('devices', 'devices')]


def _is_running(container):
    if container is None:
        return False

    client = docker_client()
    inspect = client.inspect_container(container)

    try:
        return inspect['State']['Running']
    except KeyError:
        return False


def _is_stopped(container):
    return not _is_running(container)


def _to_upper_case(key):
            return key[0].upper() + key[1:]


class DockerCompute(KindBasedMixin, BaseComputeDriver):
    def __init__(self):
        KindBasedMixin.__init__(self, kind='docker')
        BaseComputeDriver.__init__(self)
        self.host_info = HostInfo()

    @staticmethod
    def get_container_by(func):
        c = docker_client()
        containers = c.containers(all=True, trunc=False)
        containers = filter(func, containers)

        if len(containers) > 0:
            return containers[0]

        return None

    def on_ping(self, ping, pong):
        if not DockerConfig.docker_enabled():
            return

        self._add_resources(ping, pong)
        self._add_instances(ping, pong)

    def _add_instances(self, ping, pong):
        if not utils.ping_include_instances(ping):
            return

        containers = []
        for c in docker_client().containers():
            names = c.get('Names')
            if names is None:
                continue

            for name in names:
                if name.startswith('/'):
                    name = name[1:]
                    containers.append({
                        'type': 'instance',
                        'uuid': name,
                        'state': 'running',
                        'dockerId': c.get('Id')
                    })

        utils.ping_add_resources(pong, *containers)
        utils.ping_set_option(pong, 'instances', True)

    def _add_resources(self, ping, pong):
        if not utils.ping_include_resources(ping):
            return

        stats = None
        if utils.ping_include_stats(ping):
            try:
                stats = self.host_info.collect_data()
            except:
                log.exception("Error geting host info stats")

        physical_host = Config.physical_host()

        compute = {
            'type': 'host',
            'kind': 'docker',
            'name': Config.hostname(),
            'physicalHostUuid': physical_host['uuid'],
            'uuid': DockerConfig.docker_uuid(),
            'info': stats
        }

        pool = {
            'type': 'storagePool',
            'kind': 'docker',
            'name': compute['name'] + ' Storage Pool',
            'hostUuid': compute['uuid'],
            'uuid': compute['uuid'] + '-pool'
        }

        ip = {
            'type': 'ipAddress',
            'uuid': DockerConfig.docker_host_ip(),
            'address': DockerConfig.docker_host_ip(),
            'hostUuid': compute['uuid'],
        }

        proxy = Config.host_proxy()
        if proxy is not None:
            compute['apiProxy'] = proxy

        utils.ping_add_resources(pong, physical_host, compute, pool, ip)

    def inspect(self, container):
        return docker_client().inspect_container(container)

    @staticmethod
    def _name_filter(name, container):
        names = container.get('Names')
        if names is None:
            return False
        return name in names

    @staticmethod
    def _id_filter(id, container):
        container_id = container.get('Id')
        return id == container_id

    def get_container(self, instance):
        try:
            if instance.externalId is not None:
                return self.get_container_by(
                    lambda x: self._id_filter(instance.externalId, x))
        except AttributeError:
            pass

        name = '/{0}'.format(instance.uuid)
        return self.get_container_by(lambda x: self._name_filter(name, x))

    def _is_instance_active(self, instance, host):
        container = self.get_container(instance)
        return _is_running(container)

    @staticmethod
    def _setup_command(create_config, instance):
        command = ""
        try:
            command = instance.data.fields.command
        except (KeyError, AttributeError):
            return None

        if command is None or len(command.strip()) == 0:
            return None

        command_args = []
        try:
            command_args = instance.data.fields.commandArgs
        except (KeyError, AttributeError):
            pass

        if command_args is not None and len(command_args) > 0:
            command = [command]
            command.extend(command_args)

        if command is not None:
            create_config['command'] = command

    @staticmethod
    def _setup_links(start_config, instance):
        links = {}

        if 'instanceLinks' not in instance:
            return

        for link in instance.instanceLinks:
            if link.targetInstanceId is not None:
                links[link.targetInstance.uuid] = link.linkName

        start_config['links'] = links

    @staticmethod
    def _setup_ports(create_config, instance):
        ports = []
        try:
            for port in instance.ports:
                ports.append((port.privatePort, port.protocol))
        except (AttributeError, KeyError):
            pass

        if len(ports) > 0:
            create_config['ports'] = ports

    def _record_rancher_container_state(self, instance, docker_id=None):
        if docker_id is None:
            container = self.get_container(instance)
            docker_id = container['Id']

        cont_dir = Config.container_state_dir()

        file_path = path.join(cont_dir, docker_id)
        if os.path.exists(file_path):
            return

        if not os.path.exists(cont_dir):
            os.makedirs(cont_dir)

        with open(file_path, 'w') as outfile:
            marshaller = get_type(MARSHALLER)
            data = marshaller.to_string(instance)
            outfile.write(data)

    def instance_activate(self, req=None, instanceHostMap=None,
                          processData=None, **kw):

        instance = instanceHostMap.instance
        host = instanceHostMap.host
        progress = Progress(req)
        if instance is not None:
            instance.processData = processData

        if self._is_instance_active(instance, host):
            self._record_rancher_container_state(instance)
            return self._reply(req,
                               self._get_response_data(req, instanceHostMap))

        with lock(instance):
            if self._is_instance_active(instance, host):
                self._record_rancher_container_state(instance)
                return self._reply(req, self.
                                   _get_response_data(req, instanceHostMap))

            noop = self._check_noop(req)

            self._do_instance_activate(instance, host, progress, noop)

            data = self._get_response_data(req, instanceHostMap)

            return self._reply(req, data)

    def _do_instance_activate(self, instance, host, progress, noop=False):
        container = None

        if not noop:

            try:
                image_tag = instance.image.data.dockerImage.fullName
            except KeyError:
                raise Exception('Can not start container with no image')

            name = instance.uuid

            create_config = {
                'name': name,
                'detach': True
            }

            start_config = {
                'publish_all_ports': False,
                'privileged': self._is_privileged(instance)
            }

            # These _setup_simple_config_fields calls should happen before all
            # other config because they stomp over config fields that other
            # setup methods might append to. Example: the environment field
            self._setup_simple_config_fields(create_config, instance,
                                             CREATE_CONFIG_FIELDS)

            self._setup_simple_config_fields(start_config, instance,
                                             START_CONFIG_FIELDS)

            add_label(create_config, RANCHER_UUID=instance.uuid)

            self._setup_hostname(create_config, instance)

            self._setup_command(create_config, instance)

            self._setup_ports(create_config, instance)

            self._setup_volumes(create_config, instance, start_config)

            self._setup_restart_policy(instance, start_config)

            self._setup_links(start_config, instance)

            self._setup_networking(instance, host, create_config, start_config)

            setup_cattle_config_url(instance, create_config)

            client = docker_client()

            container = self._create_container(client, create_config,
                                               image_tag, instance, name,
                                               progress)
            container_id = container['Id']

            log.info('Starting docker container [%s] docker id [%s] %s', name,
                     container_id, start_config)

            client.start(container_id, **start_config)

        if container is None:
            container = self.get_container(instance)

        if container is not None:
            self._record_rancher_container_state(instance,
                                                 docker_id=container['Id'])

    def _check_noop(self, req=None):
        try:
            return req['context']['topProcessName'] == 'containerevent.create'
        except (TypeError, AttributeError, KeyError):
            return False

    def _create_container(self, client, create_config, image_tag, instance,
                          name, progress):
        container = self.get_container(instance)
        if container is None:
            log.info('Creating docker container [%s] from config %s', name,
                     create_config)

            try:
                container = client.create_container(image_tag, **create_config)
            except APIError as e:
                if e.message.response.status_code == 404:
                    pull_image(instance.image, progress)
                    container = client.create_container(image_tag,
                                                        **create_config)
                else:
                    raise
        return container

    def _setup_simple_config_fields(self, config, instance, fields):
        for src, dest in fields:
            try:
                config[dest] = instance.data.fields[src]
            except (KeyError, AttributeError):
                pass

    def _setup_volumes(self, create_config, instance, start_config):
        try:
            volumes = instance.data.fields['dataVolumes']
            volumes_map = {}
            binds_map = {}
            if volumes is not None and len(volumes) > 0:
                for i in volumes:
                    parts = i.split(':', 3)
                    if len(parts) == 1:
                        volumes_map[parts[0]] = {}
                    else:
                        read_only = len(parts) == 3 and parts[2] == 'ro'
                        bind = {'bind': parts[1], 'ro': read_only}
                        binds_map[parts[0]] = bind
                create_config['volumes'] = volumes_map
                start_config['binds'] = binds_map
        except (KeyError, AttributeError):
            pass
        try:
            # TODO Fix this to not use uuid
            vfcs = instance['dataVolumesFromContainers']
            container_names = [vfc['uuid'] for vfc in vfcs]
            if container_names:
                start_config['volumes_from'] = container_names
        except KeyError:
            pass

    def _setup_restart_policy(self, instance, start_config):
        try:
            restart_policy = instance.data.fields['restartPolicy']
            refactored_res_policy = {}
            for res_pol_key in restart_policy.keys():
                refactored_res_policy[_to_upper_case(res_pol_key)] = \
                    restart_policy[res_pol_key]
            start_config['restart_policy'] = refactored_res_policy
        except (KeyError, AttributeError):
            pass

    def _setup_hostname(self, create_config, instance):
        try:
            create_config['hostname'] = instance.hostname
        except (KeyError, AttributeError):
            pass

    def _setup_networking(self, instance, host, create_config, start_config):
        setup_mac_and_ip(instance, create_config)
        setup_ports(instance, create_config, start_config)
        setup_links(instance, create_config, start_config)
        setup_ipsec(instance, host, create_config, start_config)

    def _is_privileged(self, instance):
        try:
            return instance.data.fields['privileged']
        except (KeyError, AttributeError):
            return False

    def _get_instance_host_map_data(self, obj):
        # TODO YO
        inspect = None
        existing = self.get_container(obj.instance)
        docker_ports = {}
        docker_ip = None

        if existing is not None:
            inspect = docker_client().inspect_container(existing['Id'])
            docker_ip = inspect['NetworkSettings']['IPAddress']
            if existing.get('Ports') is not None:
                for port in existing['Ports']:
                    if 'PublicPort' in port and 'PrivatePort' not in port:
                        # Remove after docker 0.12/1.0 is released
                        private_port = '{0}/{1}'.format(port['PublicPort'],
                                                        port['Type'])
                        docker_ports[private_port] = None
                    elif 'PublicPort' in port:
                        private_port = '{0}/{1}'.format(port['PrivatePort'],
                                                        port['Type'])
                        docker_ports[private_port] = str(port['PublicPort'])
                    else:
                        private_port = '{0}/{1}'.format(port['PrivatePort'],
                                                        port['Type'])
                        docker_ports[private_port] = None

        update = {
            'instance': {
                '+data': {
                    'dockerContainer': existing,
                    'dockerInspect': inspect,
                    '+fields': {
                        'dockerHostIp': DockerConfig.docker_host_ip(),
                        'dockerPorts': docker_ports,
                        'dockerIp': docker_ip
                    }
                }
            }
        }
        if existing is not None:
            update['instance']['externalId'] = existing['Id']

        return update

    def _is_instance_inactive(self, instance, host):
        container = self.get_container(instance)
        return _is_stopped(container)

    def _do_instance_deactivate(self, instance, host, progress):
        c = docker_client()
        timeout = 10

        try:
            timeout = int(instance.processData.timeout)
        except (TypeError, KeyError, AttributeError):
            pass

        container = self.get_container(instance)

        c.stop(container['Id'], timeout=timeout)

        container = self.get_container(instance)
        if not _is_stopped(container):
            c.kill(container['Id'])

        container = self.get_container(instance)
        if not _is_stopped(container):
            raise Exception('Failed to stop container {0}'
                            .format(instance.uuid))
