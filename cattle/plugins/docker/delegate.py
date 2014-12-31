import logging
import re

from urlparse import urlparse

from cattle import Config
from cattle.agent.handler import BaseHandler
from cattle.progress import Progress
from cattle.type_manager import get_type, MARSHALLER
from cattle.utils import reply
from .compute import DockerCompute
from .util import add_to_env
from . import docker_client

import requests

log = logging.getLogger('docker')


def _make_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=Config.workers(),
                                            pool_maxsize=Config.workers())
    session.mount('http://', adapter)
    return session


_SESSION = _make_session()
_REGEX = re.compile(';.*')


def container_exec(ip, instanceData, event):
    marshaller = get_type(MARSHALLER)
    c = docker_client()
    executor = Config.home() + '/events/executor.py'
    cmd = '{0}/events/' + _REGEX.sub('', event.name)
    cmd = cmd.format(Config.home())
    cmd = [executor, cmd]
    cmd_id = c.executeCreate(instanceData.uuid, cmd,
                             stdin=True, stream=True)
    client_sock = None
    data = ''
    try:
        client_sock = c.executeStart(cmd_id, stream=True)
        client_sock.write(marshaller.to_string(event))
        data = ''.join([str(x) for x in client_sock.read()])
        client_sock.shutdown(0)
    except Exception as e:
        log.error(e)
    finally:
        client_sock.close()
    result_data = None
    try:
        result_data = data.decode('utf-8')
        result_data = marshaller.from_string(result_data)
        exitCode = result_data['exitCode']
        return exitCode, result_data['output'], result_data['data']
    except Exception as e:
        log.error(e)
        return 1, result_data, None


class DockerDelegate(BaseHandler):
    def __init__(self):
        self.compute = DockerCompute()
        pass

    def events(self):
        return ['delegate.request']

    def delegate_request(self, req=None, event=None, instanceData=None, **kw):
        if instanceData.kind != 'container' or \
           instanceData.get('token') is None:
            return

        container = self.compute.get_container_by_name(instanceData.uuid)
        if container is None:
            return

        inspect = self.compute.inspect(container)

        try:
            ip = inspect['NetworkSettings']['IPAddress']
            running = inspect['State']['Running']
            if not running:
                log.error('Can not call [%s], container is not running',
                          instanceData.uuid)
                return
        except KeyError:
            log.error('Can not call [%s], container is not running',
                      instanceData.uuid)
            return

        try:
            # Optimization for empty config.updates, should really find a
            # better way to do this
            if event.name == 'config.update' and len(event.data.items) == 0:
                return reply(event, None, parent=req)
        except:
            pass

        progress = Progress(event, parent=req)
        exit_code, output, data = container_exec(ip, instanceData, event)

        if exit_code == 0:
            return reply(event, data, parent=req)
        else:
            progress.update('Update failed', data={
                'exitCode': exit_code,
                'output': output
            })

    def before_start(self, instance, host, config, start_config):
        if instance.get('agentId') is None:
            return

        url = Config.config_url()

        if url is not None:
            parsed = urlparse(url)

            if 'localhost' == parsed.hostname:
                port = Config.api_proxy_listen_port()
                add_to_env(config,
                           CATTLE_AGENT_INSTANCE='true',
                           CATTLE_CONFIG_URL_SCHEME=parsed.scheme,
                           CATTLE_CONFIG_URL_PATH=parsed.path,
                           CATTLE_CONFIG_URL_PORT=port)
            else:
                add_to_env(config, CATTLE_CONFIG_URL=url)

    def after_start(self, instance, host, id):
        pass
