import logging

from . import docker_client, pull_image
from . import DockerConfig, DOCKER_COMPUTE_LISTENER
from cattle import Config
from cattle.compute import BaseComputeDriver
from cattle.agent.handler import KindBasedMixin
from cattle.type_manager import get_type_list
from cattle import utils
from docker.errors import APIError
from cattle.plugins.host_info.main import HostInfo

log = logging.getLogger('docker')


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
                    if utils.is_uuid(name):
                        containers.append({
                            'type': 'instance',
                            'uuid': name,
                            'state': 'running'
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
            'physicalHostStats': stats
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

        utils.ping_add_resources(pong, physical_host, compute, pool, ip)

    def inspect(self, container):
        return docker_client().inspect_container(container)

    @staticmethod
    def _name_filter(name, container):
        names = container.get('Names')
        if names is None:
            return False
        return name in names

    def get_container_by_name(self, name):
        name = '/{0}'.format(name)
        return self.get_container_by(lambda x: self._name_filter(name, x))

    def _is_instance_active(self, instance, host):
        container = self.get_container_by_name(instance.uuid)
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

    def _do_instance_activate(self, instance, host, progress):

        def to_upper_case(key):
            return key[0].upper() + key[1:]

        name = instance.uuid
        try:
            image_tag = instance.data.fields.imageUuid
            if image_tag.startswith('docker:'):
                image_tag = image_tag[7:]
        except KeyError:
            raise Exception('Can not start container with no image')

        c = docker_client()

        create_config = {
            'name': name,
            'detach': True
        }

        # Docker-py doesn't support working_dir, maybe in 0.2.4?
        create_config_fields = [
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

        for src, dest in create_config_fields:
            try:
                create_config[dest] = instance.data.fields[src]
            except (KeyError, AttributeError):
                pass

        try:
            create_config['hostname'] = instance.hostname
        except (KeyError, AttributeError):
            pass

        start_config = {
            'publish_all_ports': False,
            'privileged': self._is_privileged(instance)
        }

        start_config_fields = [
            ('capAdd', 'cap_add'),
            ('capDrop', 'cap_drop'),
            ('dnsSearch', 'dns_search'),
            ('dns', 'dns'),
            ('publishAllPorts', 'publish_all_ports'),
            ('lxcConf', 'lxc_conf')]

        for src, dest in start_config_fields:
            try:
                start_config[dest] = instance.data.fields[src]
            except (KeyError, AttributeError):
                pass

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
            vfcs = instance['dataVolumesFromContainers']
            container_names = [vfc['uuid'] for vfc in vfcs]
            if container_names:
                start_config['volumes_from'] = container_names
        except KeyError:
            pass

        try:
            devices = instance.data.fields['devices']
            start_config['devices'] = devices
        except KeyError:
            pass

        try:
            restart_policy = instance.data.fields['restartPolicy']
            refactored_res_policy = {}
            for res_pol_key in restart_policy.keys():
                refactored_res_policy[to_upper_case(res_pol_key)] = \
                    restart_policy[res_pol_key]
            start_config['restart_policy'] = refactored_res_policy
        except KeyError:
            pass
        self._setup_command(create_config, instance)
        self._setup_ports(create_config, instance)

        self._setup_links(start_config, instance)

        self._call_listeners(True, instance, host, create_config, start_config)

        container = self.get_container_by_name(name)
        if container is None:
            log.info('Creating docker container [%s]'
                     'from config: [%s] using image: [%s]',
                     name, create_config, image_tag)

            try:
                container = c.create_container(image_tag, **create_config)
            except APIError as e:
                try:
                    if e.message.response.status_code == 404:
                        # Ensure image is pulled, somebody could have deleted
                        # it behind the scenes
                        auth_config = {
                            'username': 'wizardofmath+whisper',
                            'password': 'W0IUYDBM2VORHM4DTTEHSMKLXGCG3KD3IT081'
                                        'QWWTZA11R9DZS2DDPP7248NUTT6',
                            'email': 'wizardofmath+whisper@gmail.com',
                            'serveraddress': 'https://quay.io/v1/'
                        }
                        pull_image(image_tag, progress, auth_config)
                        cc = create_config
                        container = c.create_container(image_tag, **cc)
                    else:
                        raise(e)
                except:
                    raise(e)

        log.info('Starting docker container [%s] docker id [%s] %s', name,
                 container['Id'], start_config)
        c.start(container['Id'], **start_config)

        self._call_listeners(False, instance, host, container['Id'])

    def _call_listeners(self, before, *args):
        for listener in get_type_list(DOCKER_COMPUTE_LISTENER):
            if before:
                listener.before_start(*args)
            else:
                listener.after_start(*args)

    def _is_privileged(self, instance):
        try:
            return instance.data.fields['privileged']
        except (KeyError, AttributeError):
            return False

    def _get_instance_host_map_data(self, obj):
        inspect = None
        existing = self.get_container_by_name(obj.instance.uuid)
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

        return {
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

    def _is_instance_inactive(self, instance, host):
        name = instance.uuid
        container = self.get_container_by_name(name)

        return _is_stopped(container)

    def _do_instance_deactivate(self, instance, host, progress):
        name = instance.uuid
        c = docker_client()
        timeout = 10

        try:
            timeout = int(instance.processData.timeout)
        except (TypeError, KeyError, AttributeError):
            pass

        container = self.get_container_by_name(name)

        c.stop(container['Id'], timeout=timeout)

        container = self.get_container_by_name(name)
        if not _is_stopped(container):
            c.kill(container['Id'])

        container = self.get_container_by_name(name)
        if not _is_stopped(container):
            raise Exception('Failed to stop container {0}'
                            .format(name))
