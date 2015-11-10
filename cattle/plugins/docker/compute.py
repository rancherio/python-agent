import logging
import socket
from os import path, remove, makedirs, rename, environ

from . import docker_client, pull_image
from . import DockerConfig
from . import DockerPool
from cattle import Config
from cattle.compute import BaseComputeDriver
from cattle.agent.handler import KindBasedMixin
from cattle.type_manager import get_type, MARSHALLER
from cattle import utils
from cattle.utils import JsonObject
from docker.errors import APIError
from docker import tls
from cattle.plugins.host_info.main import HostInfo
from cattle.plugins.docker.util import add_label, is_no_op, remove_container
from cattle.progress import Progress
from cattle.lock import lock
from cattle.plugins.docker.network import setup_ipsec, setup_links, \
    setup_mac_and_ip, setup_ports, setup_network_mode
from cattle.plugins.docker.agent import setup_cattle_config_url


log = logging.getLogger('docker')

SYSTEM_LABEL = 'io.rancher.container.system'

CREATE_CONFIG_FIELDS = [
    ('labels', 'labels'),
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
    ('workingDir', 'working_dir'),
    ('entryPoint', 'entrypoint')]

START_CONFIG_FIELDS = [
    ('capAdd', 'cap_add'),
    ('capDrop', 'cap_drop'),
    ('dnsSearch', 'dns_search'),
    ('dns', 'dns'),
    ('extraHosts', 'extra_hosts'),
    ('publishAllPorts', 'publish_all_ports'),
    ('lxcConf', 'lxc_conf'),
    ('logConfig', 'log_config'),
    ('securityOpt', 'security_opt'),
    ('restartPolicy', 'restart_policy'),
    ('pidMode', 'pid_mode'),
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
        self.host_info = HostInfo(docker_client())
        self.system_images = self.get_agent_images(docker_client())

    def get_agent_images(self, client):
        images = client.images(filters={'label': SYSTEM_LABEL})
        system_images = {}
        for i in images:
            try:
                label_val = i['Labels'][SYSTEM_LABEL]
                for l in i['RepoTags']:
                    system_images[l] = label_val
                    if l.endswith(':latest'):
                        alias = l[:-7]
                        system_images[alias] = label_val
            except KeyError:
                pass
        return system_images

    @staticmethod
    def get_container_by(client, func):
        containers = client.containers(all=True, trunc=False)
        containers = filter(func, containers)

        if len(containers) > 0:
            return containers[0]

        return None

    @staticmethod
    def find_first(containers, func):
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

        utils.ping_add_resources(pong, {
            'type': 'hostUuid',
            'uuid': DockerConfig.docker_uuid()
        })

        containers = []
        running, nonrunning = self._get_all_containers_by_state()

        for key, container in running.iteritems():
            self.add_container('running', container, containers)

        for key, container in nonrunning.iteritems():
            self.add_container('stopped', container, containers)

        utils.ping_add_resources(pong, *containers)
        utils.ping_set_option(pong, 'instances', True)

    def add_container(self, state, container, containers):
        try:
            labels = container['Labels']
        except KeyError:
            labels = []

        container_data = {
            'type': 'instance',
            'uuid': self._get_uuid(container),
            'state': state,
            'systemContainer': self._get_sys_container(container),
            'dockerId': container['Id'],
            'image': container['Image'],
            'labels': labels,
            'created': container['Created'],
        }
        containers.append(container_data)

    def _get_all_containers_by_state(self):
        client = docker_client()

        nonrunning_containers = {}
        for c in client.containers(all=True):
            # Blank status only wait to distinguish created from stopped
            if c['Status'] != '' and c['Status'] != 'Created':
                nonrunning_containers[c['Id']] = c

        running_containers = {}
        for c in client.containers(all=False):
            running_containers[c['Id']] = c
            del nonrunning_containers[c['Id']]

        return running_containers, nonrunning_containers

    def _get_sys_container(self, container):
        try:
            image = container['Image']
            if image in self.system_images:
                return self.system_images[image]
        except (TypeError, KeyError):
            pass

        try:
            return container['Labels']['io.rancher.container.system']
        except (TypeError, KeyError):
            pass

    def _get_uuid(self, container):
        try:
            uuid = container['Labels']['io.rancher.container.uuid']
            if uuid:
                return uuid
        except (TypeError, KeyError):
            pass

        names = container['Names']
        if not names:
            # No name?? Make one up
            return 'no-uuid-%s' % container['Id']

        if names[0].startswith('/'):
            return names[0][1:]
        else:
            return names[0]

    def _determine_state(self, container):
        status = container['Status']
        if status == '' or (status is not None and
                            status.lower() == 'created'):
            return 'created'
        elif 'Up ' in status:
            return 'running'
        elif 'Exited ' in status:
            return 'stopped'
        else:
            # Unknown. Assume running and state should sync up eventually.
            return 'running'

    def _get_host_labels(self):
        try:
            labels = self.host_info.host_labels()
        except:
            log.exception("Error getting host labels")

        if Config.labels():
            labels.update(Config.labels())

        return labels

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
            'labels': self._get_host_labels(),
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

        resolved_ip = socket.gethostbyname(DockerConfig.docker_host_ip())

        ip = {
            'type': 'ipAddress',
            'uuid': resolved_ip,
            'address': resolved_ip,
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

    @staticmethod
    def _agent_id_filter(id, container):
        try:
            return container['Labels']['io.rancher.container.agent_id'] == id
        except (TypeError, KeyError, AttributeError):
            pass

    def get_container(self, client, instance, by_agent=False):
        if instance is None:
            return None

        name = '/{0}'.format(instance.uuid)
        container_list = client.containers(all=True, trunc=False)
        container = self.find_first(container_list,
                                    lambda x: self._name_filter(name, x))
        if container:
            return container

        if hasattr(instance, 'externalId') and instance.externalId:
            f = lambda x: self._id_filter(instance.externalId, x)
            container = self.find_first(container_list, f)

        if container:
            return container

        if by_agent and hasattr(instance, 'agentId') and instance.agentId:
            f = lambda x: self._agent_id_filter(str(instance.agentId), x)
            container = self.find_first(container_list, f)

        return container

    def _is_instance_active(self, instance, host):
        if is_no_op(instance):
            return True

        client = self._get_docker_client(host)
        container = self.get_container(client, instance)
        return _is_running(client, container)

    @staticmethod
    def _get_docker_client(host, version_override=None):
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
            version=version_override,
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
    def _setup_ports(create_config, instance, start_config):
        ports = []
        bindings = {}
        try:
            for port in instance.ports:
                ports.append((port.privatePort, port.protocol))
                if port.publicPort is not None:
                    bind = '{0}/{1}'.format(port.privatePort, port.protocol)
                    bindings[bind] = ('', port.publicPort)
        except (AttributeError, KeyError):
            pass

        if len(ports) > 0:
            create_config['ports'] = ports

        if len(bindings) > 0:
            start_config['port_bindings'] = bindings

    def _record_state(self, client, instance, docker_id=None):
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

    def purge_state(self, client, instance):
        container = self.get_container(client, instance)
        if container is None:
            return

        docker_id = container['Id']
        cont_dir = Config.container_state_dir()
        files = [path.join(cont_dir, 'tmp-%s' % docker_id),
                 path.join(cont_dir, docker_id)]

        for f in files:
            if path.exists(f):
                remove(f)

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
                self._record_state(client, instance)
                return self._reply(req, self.
                                   _get_response_data(req, instanceHostMap))

            self._do_instance_activate(instance, host, progress)

            data = self._get_response_data(req, instanceHostMap)

            return self._reply(req, data)

    def _do_instance_activate(self, instance, host, progress):
        if is_no_op(instance):
            return

        client = self._get_docker_client(host)

        image_tag = self._get_image_tag(instance)

        name = instance.uuid

        create_config = {
            'name': name,
            'detach': True
        }

        start_config = {
            'publish_all_ports': False,
            'privileged': self._is_true(instance, 'privileged'),
            'read_only': self._is_true(instance, 'readOnly'),
        }

        # These _setup_simple_config_fields calls should happen before all
        # other config because they stomp over config fields that other
        # setup methods might append to. Example: the environment field
        self._setup_simple_config_fields(create_config, instance,
                                         CREATE_CONFIG_FIELDS)

        self._setup_simple_config_fields(start_config, instance,
                                         START_CONFIG_FIELDS)

        add_label(create_config, {'io.rancher.container.uuid': instance.uuid})

        self._setup_logging(start_config, instance)

        self._setup_hostname(create_config, instance)

        self._setup_command(create_config, instance)

        self._setup_ports(create_config, instance, start_config)

        self._setup_volumes(create_config, instance, start_config, client)

        self._setup_links(start_config, instance)

        self._setup_networking(instance, host, create_config, start_config)

        self._flag_system_container(instance, create_config)

        self._setup_proxy(instance, create_config)

        setup_cattle_config_url(instance, create_config)

        create_config['host_config'] = \
            client.create_host_config(**start_config)

        container = self._create_container(client, create_config,
                                           image_tag, instance, name,
                                           progress)
        container_id = container['Id']

        log.info('Starting docker container [%s] docker id [%s] %s', name,
                 container_id, start_config)

        client.start(container_id)

        self._record_state(client, instance, docker_id=container['Id'])

    def _create_container(self, client, create_config, image_tag, instance,
                          name, progress):
        container = self.get_container(client, instance)
        if container is None:
            log.info('Creating docker container [%s] from config %s', name,
                     create_config)

            labels = create_config['labels']
            if labels.get('io.rancher.container.pull_image', None) == 'always':
                self._do_instance_pull(JsonObject({
                    'image': instance.image,
                    'tag': None,
                    'mode': 'all',
                    'complete': False,
                }), progress)
            try:
                del create_config['name']
                command = ''
                try:
                    command = create_config['command']
                    del create_config['command']
                except KeyError:
                    pass
                config = client.create_container_config(image_tag,
                                                        command,
                                                        **create_config)
                try:
                    id = instance.data
                    config['VolumeDriver'] = id.fields['volumeDriver']
                except (KeyError, AttributeError):
                    pass
                container = client.create_container_from_config(config, name)
            except APIError as e:
                if e.message.response.status_code == 404:
                    pull_image(instance.image, progress)
                    container = client.create_container_from_config(config,
                                                                    name)
                else:
                    raise
        return container

    def _flag_system_container(self, instance, create_config):
        try:
            if instance.systemContainer:
                add_label(create_config, {
                    'io.rancher.container.system': instance.systemContainer})
        except (KeyError, AttributeError):
            pass

    def _setup_proxy(self, instance, create_config):
        try:
            if instance.systemContainer:
                if 'environment' not in create_config:
                    create_config['environment'] = {}

                for i in ['http_proxy', 'https_proxy', 'NO_PROXY']:
                    try:
                        create_config['environment'][i] = environ[i]
                    except KeyError:
                        pass
        except (KeyError, AttributeError):
            pass

    def _setup_simple_config_fields(self, config, instance, fields):
        for src, dest in fields:
            try:
                src_obj = instance.data.fields[src]
                config[dest] = JsonObject.unwrap(src_obj)
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

        try:
            for v in instance['volumesFromDataVolumeMounts']:
                if not DockerPool.is_volume_active(v):
                    DockerPool.do_volume_activate(v)
        except KeyError:
            pass

    def _get_image_tag(self, instance):
        try:
            return instance.image.data.dockerImage.fullName
        except (KeyError, AttributeError):
            raise Exception('Can not start container with no image')

    def _setup_logging(self, start_config, instance):
        try:
            if start_config.get('log_config', None) is None:
                return

            type = start_config['log_config']['driver']
            del start_config['log_config']['driver']
            start_config['log_config']['type'] = type
        except (KeyError, AttributeError):
            pass

        for i in ['type', 'config']:
            bad = True
            try:
                obj = start_config['log_config'][i]
                if obj is not None:
                    bad = False
                    start_config['log_config'][i] = JsonObject.unwrap(obj)
            except (KeyError, AttributeError):
                pass
            if bad and 'log_config' in start_config:
                del start_config['log_config']

    def _setup_hostname(self, create_config, instance):
        try:
            create_config['hostname'] = instance.hostname
        except (KeyError, AttributeError):
            pass

    def _setup_networking(self, instance, host, create_config, start_config):
        client = self._get_docker_client(host)

        managed = setup_network_mode(instance, self, client, create_config,
                                     start_config)
        setup_mac_and_ip(instance, create_config, set_mac=managed)
        setup_ports(instance, create_config, start_config, managed)
        setup_links(instance, create_config, start_config)
        setup_ipsec(instance, host, create_config, start_config)

    def _is_true(self, instance, key):
        try:
            return instance.data.fields[key] is True
        except (KeyError, AttributeError):
            return False

    def _get_instance_host_map_data(self, obj):
        client = self._get_docker_client(obj.host)
        inspect = None
        docker_mounts = None
        existing = self.get_container(client, obj.instance)
        docker_ports = {}
        docker_ip = None

        if existing is not None:
            inspect = client.inspect_container(existing['Id'])
            docker_mounts = self._get_mount_data(obj.host, existing['Id'])
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

        if docker_mounts is not None:
            update['instance']['+data']['dockerMounts'] = docker_mounts

        return update

    def _get_mount_data(self, host, container_id):
        try:
            client = self._get_docker_client(host, '1.21')
            inspect = client.inspect_container(container_id)
            return inspect['Mounts']
        except (KeyError, APIError):
            pass

    def _is_instance_inactive(self, instance, host):
        if is_no_op(instance):
            return True

        c = self._get_docker_client(host)
        container = self.get_container(c, instance)

        return _is_stopped(c, container)

    def _do_instance_deactivate(self, instance, host, progress):
        if is_no_op(instance):
            return

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

    def _do_instance_force_stop(self, instanceForceStop):
        try:
            docker_client().stop(instanceForceStop['id'])
        except APIError as e:
            if e.message.response.status_code != 404:
                raise e

    def _is_instance_removed(self, instance, host):
        client = self._get_docker_client(host)
        container = self.get_container(client, instance)
        return container is None

    def _do_instance_remove(self, instance, host, progress):
        client = self._get_docker_client(host)
        container = self.get_container(client, instance)
        if container is None:
            return

        remove_container(client, container)

    def _do_instance_pull(self, pull_info, progress):
        client = docker_client()

        image = pull_info.image.data.dockerImage
        try:
            existing = client.inspect_image(image.fullName)
        except APIError:
            existing = None

        if pull_info.mode == 'cached' and existing is None:
            return existing

        if pull_info.complete:
            if existing is not None:
                client.remove_image(image.fullName + pull_info.tag)
            return

        DockerPool.image_pull(pull_info.image, progress)

        if pull_info.tag is not None:
            image_info = DockerPool.parse_repo_tag(image.fullName)
            client.tag(image.fullName, image_info['repo'],
                       image_info['tag'] + pull_info.tag, force=True)
        return client.inspect_image(image.fullName)

    def _do_instance_inspect(self, instanceInspectRequest):
        client = docker_client()
        container = None
        try:
            container_id = instanceInspectRequest.id
            container = self.get_container_by(client,
                                              lambda x: self._id_filter(
                                                  container_id, x))
        except (KeyError, AttributeError):
            pass

        if not container:
            try:
                name = '/{0}'.format(instanceInspectRequest.name)
                container = self.get_container_by(client,
                                                  lambda x: self._name_filter(
                                                      name, x))
            except (KeyError, AttributeError):
                pass

        if container:
            inspect = client.inspect_container(container)
            return inspect
