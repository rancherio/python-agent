import logging
from os import path, remove, makedirs, rename

from . import docker_client, pull_image
from . import DockerConfig
from cattle import Config
from cattle.compute import BaseComputeDriver
from cattle.agent.handler import KindBasedMixin
from cattle.type_manager import get_type, MARSHALLER
from cattle import utils
from docker.errors import APIError
from docker import tls
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


def _is_running(client, container):
    if container is None:
        return False
    inspect = client.inspect_container(container)

    try:
        return inspect['State']['Running']
    except KeyError:
        return False


def _is_stopped(client, container):
    return not _is_running(client, container)


def _to_upper_case(key):
            return key[0].upper() + key[1:]


class DockerCompute(KindBasedMixin, BaseComputeDriver):
    def __init__(self):
        KindBasedMixin.__init__(self, kind='docker')
        BaseComputeDriver.__init__(self)
        self.host_info = HostInfo()

    @staticmethod
    def get_container_by(client, func):
        containers = client.containers(all=True, trunc=False)
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
        found = False
        for n in names:
            if n.endswith(name):
                found = True
                break
        return found

    @staticmethod
    def _id_filter(id, container):
        container_id = container.get('Id')
        return id == container_id

    def get_container_by_id(self, client, id):
        return self.get_container_by(client, lambda x: self._id_filter(id, x))

    def get_container(self, client, instance):
        try:
            if instance.externalId is not None:
                return self.get_container_by(client, lambda x: self._id_filter(
                    instance.externalId, x))
        except AttributeError:
            pass

        name = '/{0}'.format(instance.uuid)
        return self.get_container_by(client,
                                     lambda x: self._name_filter(name, x))

    def _is_instance_active(self, instance, host):
        client = self._get_docker_client(host)
        container = self.get_container(client, instance)
        return _is_running(client, container)

    @staticmethod
    def _get_docker_client(host):
        cluster_connection = None
        tls_config = None
        try:
            cluster_connection = host['clusterConnection']
            if cluster_connection.startswith('https'):
                try:
                    account_id = host['accountId']
                    ca_crt = host['caCrt']
                    client_crt = host['clientCrt']
                    client_key = host['clientKey']

                    client_certs_dir = Config.client_certs_dir()
                    acct_client_cert_dir = \
                        path.join(client_certs_dir, str(account_id))
                    if not path.exists(acct_client_cert_dir):
                        log.debug('Creating client cert directory: %s',
                                  acct_client_cert_dir)
                        makedirs(acct_client_cert_dir)
                    if ca_crt:
                        log.debug('Writing cert auth')
                        with open(path.join(acct_client_cert_dir, 'ca.crt'),
                                  'w') as f:
                            f.write(ca_crt)
                    if client_crt:
                        log.debug('Writing client cert')
                        with open(path.join(acct_client_cert_dir,
                                            'client.crt'),
                                  'w') as f:
                                f.write(client_crt)
                    if client_key:
                        log.debug('Writing client key')
                        with open(path.join(acct_client_cert_dir,
                                            'client.key'),
                                  'w') as f:
                            f.write(client_key)
                    if ca_crt and client_crt and client_key:
                        tls_config = tls.TLSConfig(
                            client_cert=(
                                path.join(acct_client_cert_dir, 'client.crt'),
                                path.join(acct_client_cert_dir, 'client.key')
                            ),
                            verify=path.join(acct_client_cert_dir, 'ca.crt'),
                            assert_hostname=False
                        )
                except (KeyError, AttributeError) as e:
                    raise Exception(
                        'Unable to process cert/keys for cluster',
                        cluster_connection,
                        e)
        except (KeyError, AttributeError):
            pass

        return docker_client(
            base_url_override=cluster_connection,
            tls_config=tls_config)

    @staticmethod
    def _setup_legacy_command(create_config, instance, command):
        # This can be removed shortly once cattle removes
        # commandArgs
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
    def _setup_command(create_config, instance):
        command = ""
        try:
            command = instance.data.fields.command
        except (KeyError, AttributeError):
            return None

        if isinstance(command, basestring):
            DockerCompute._setup_legacy_command(create_config, instance,
                                                command)
        else:
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

    def _record_rancher_container_state(self, client, instance,
                                        docker_id=None):
        if docker_id is None:
            container = self.get_container(client, instance)
            if container is not None:
                docker_id = container['Id']

        if docker_id is None:
            return

        cont_dir = Config.container_state_dir()

        tmp_file_path = path.join(cont_dir, 'tmp-%s' % docker_id)
        if path.exists(tmp_file_path):
            remove(tmp_file_path)

        file_path = path.join(cont_dir, docker_id)
        if path.exists(file_path):
            remove(file_path)

        if not path.exists(cont_dir):
            makedirs(cont_dir)

        with open(tmp_file_path, 'w') as outfile:
            marshaller = get_type(MARSHALLER)
            data = marshaller.to_string(instance)
            outfile.write(data)

        rename(tmp_file_path, file_path)

    def instance_activate(self, req=None, instanceHostMap=None,
                          processData=None, **kw):
        instance, host = \
            BaseComputeDriver.get_instance_host_from_map(self, instanceHostMap)

        progress = Progress(req)
        client = self._get_docker_client(host)
        if instance is not None:
            instance.processData = processData

        with lock(instance):
            if self._is_instance_active(instance, host):
                self._record_rancher_container_state(client, instance)
                return self._reply(req, self.
                                   _get_response_data(req, instanceHostMap))

            no_op = self._is_no_op(instance)

            container = self._do_instance_activate(instance, host, progress,
                                                   no_op)

            data = self._get_response_data(req, instanceHostMap,
                                           container=container)

            return self._reply(req, data)

    def _do_instance_activate(self, instance, host, progress, no_op=False):
        container = None
        client = self._get_docker_client(host)

        if not no_op:
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

            self._setup_volumes(create_config, instance, start_config, client)

            self._setup_restart_policy(instance, start_config)

            self._setup_links(start_config, instance)

            self._setup_networking(instance, host, create_config, start_config)

            setup_cattle_config_url(instance, create_config)

            container = self._create_container(client, create_config,
                                               image_tag, instance, name,
                                               progress)
            container_id = container['Id']

            log.info('Starting docker container [%s] docker id [%s] %s', name,
                     container_id, start_config)

            client.start(container_id, **start_config)

        if container is None:
            container = self.get_container(client, instance)

        if container is not None:
            self._record_rancher_container_state(client, instance,
                                                 docker_id=container['Id'])
            return self.get_container_by_id(client, container['Id'])

    def _is_no_op(self, instance):
        try:
            return instance.processData.containerNoOpEvent
        except (AttributeError, KeyError):
            return False

    def _create_container(self, client, create_config, image_tag, instance,
                          name, progress):
        container = self.get_container(client, instance)
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

    def _setup_volumes(self, create_config, instance, start_config, client):
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
            containers = []
            for vfc in instance['dataVolumesFromContainers']:
                container = self.get_container(client, vfc)
                if container:
                    containers.append(container['Id'])
            if containers:
                start_config['volumes_from'] = containers
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

    def _get_instance_host_map_data(self, obj, container=None):
        client = self._get_docker_client(obj.host)
        inspect = None
        if container:
            existing = container
        else:
            existing = self.get_container(client, obj.instance)
        docker_ports = {}
        docker_ip = None

        if existing is not None:
            inspect = client.inspect_container(existing['Id'])
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
        c = self._get_docker_client(host)
        container = self.get_container(c, instance)

        return _is_stopped(c, container)

    def _do_instance_deactivate(self, instance, host, progress):
        c = self._get_docker_client(host)
        timeout = 10

        try:
            timeout = int(instance.processData.timeout)
        except (TypeError, KeyError, AttributeError):
            pass

        container = self.get_container(c, instance)

        c.stop(container['Id'], timeout=timeout)

        container = self.get_container(c, instance)
        if not _is_stopped(c, container):
            c.kill(container['Id'])

        container = self.get_container(c, instance)
        if not _is_stopped(c, container):
            raise Exception('Failed to stop container {0}'
                            .format(instance.uuid))
