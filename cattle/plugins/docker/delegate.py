import logging

from urlparse import urlparse

from cattle import Config
from cattle.utils import reply
from .util import add_to_env
from .compute import DockerCompute
from cattle.agent.handler import BaseHandler
from cattle.progress import Progress
from cattle.type_manager import get_type, MARSHALLER
from . import DockerConfig, docker_client

import requests

log = logging.getLogger('docker')


def _make_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=Config.workers(),
                                            pool_maxsize=Config.workers())
    session.mount('http://', adapter)
    return session


_SESSION = _make_session()


def container_exec(ip, instanceData, event):
    if event.name == 'config.update':
        c = docker_client()
        clientSock = c.execute(instanceData.uuid,
                               '{0}/config.update'.format(Config.home()),
                               False, True, True, True, True, True, True)
        data = clientSock.read()
        return 0, data, data

    else:
        marshaller = get_type(MARSHALLER)
        data = marshaller.to_string(event)
        url = 'http://{0}:8080/events?token={1}'.format(ip, instanceData.token)

        r = _SESSION.post(url, data=data, headers={
            'Content-Type': 'application/json'
        }, timeout=DockerConfig.delegate_timeout())

        if r.status_code != 200:
            return r.status_code, r.text, None

        result = r.json()

        data = result.get('data')
        if data is not None:
            data = marshaller.from_string(data)

        return result.get('exitCode'), result.get('output'), data


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
