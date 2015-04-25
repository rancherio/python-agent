import os
import socket

from os import path
from uuid import uuid4

from cattle.utils import memoize

CONFIG_OVERRIDE = {}


try:
    import eventlet  # NOQA
except:
    pass


def default_value(name, default):
    if name in CONFIG_OVERRIDE:
        return CONFIG_OVERRIDE[name]
    result = os.environ.get('CATTLE_%s' % name, default)
    if result == '':
        return default
    return result


_SCHEMAS = '/schemas'


def _strip_schemas(url):
    if url is None:
        return None

    if url.endswith(_SCHEMAS):
        return url[0:len(url)-len(_SCHEMAS)]

    return url


class Config:
    def __init__(self):
        pass

    @staticmethod
    @memoize
    def _get_uuid_from_file(uuid_file):
        uuid = None

        if path.exists(uuid_file):
            with open(uuid_file) as f:
                uuid = f.read().strip()
            if len(uuid) == 0:
                uuid = None

        if uuid is None:
            uuid = str(uuid4())
            with open(uuid_file, 'w') as f:
                f.write(uuid)

        return uuid

    @staticmethod
    def state_dir():
        return default_value('STATE_DIR', Config.home())

    @staticmethod
    def physical_host_uuid_file():
        def_value = '{0}/.physical_host_uuid'.format(Config.state_dir())
        return default_value('PHYSICAL_HOST_UUID_FILE', def_value)

    @staticmethod
    def physical_host_uuid():
        return Config.get_uuid_from_file('PHYSICAL_HOST_UUID',
                                         Config.physical_host_uuid_file())

    @staticmethod
    def setup_logger():
        return default_value('LOGGER', 'true') == 'true'

    @staticmethod
    def do_ping():
        return default_value('PING_ENABLED', 'true') == 'true'

    @staticmethod
    def get_uuid_from_file(env_name, uuid_file):
        uuid = default_value(env_name, None)
        if uuid is not None:
            return uuid

        return Config._get_uuid_from_file(uuid_file)

    @staticmethod
    def hostname():
        return default_value('HOSTNAME', socket.gethostname())

    @staticmethod
    def workers():
        return int(default_value('WORKERS', '50'))

    @staticmethod
    def set_secret_key(value):
        CONFIG_OVERRIDE['SECRET_KEY'] = value

    @staticmethod
    def secret_key():
        return default_value('SECRET_KEY', 'adminpass')

    @staticmethod
    def set_access_key(value):
        CONFIG_OVERRIDE['ACCESS_KEY'] = value

    @staticmethod
    def access_key():
        return default_value('ACCESS_KEY', 'admin')

    @staticmethod
    def set_api_url(value):
        CONFIG_OVERRIDE['URL'] = value

    @staticmethod
    def api_url(default=None):
        return _strip_schemas(default_value('URL', default))

    @staticmethod
    def api_auth():
        return Config.access_key(), Config.secret_key()

    @staticmethod
    def config_url():
        ret = default_value('CONFIG_URL', None)
        if ret is None:
            return Config.api_url()
        else:
            return ret

    @staticmethod
    def is_multi_proc():
        return Config.multi_style() == 'proc'

    @staticmethod
    def is_multi_thread():
        return Config.multi_style() == 'thread'

    @staticmethod
    def is_eventlet():
        if 'eventlet' not in globals():
            return False

        setting = default_value('AGENT_MULTI', None)

        if setting is None or setting == 'eventlet':
            return True

        return False

    @staticmethod
    def multi_style():
        return default_value('AGENT_MULTI', 'proc')

    @staticmethod
    def queue_depth():
        return int(default_value('QUEUE_DEPTH', 5))

    @staticmethod
    def stop_timeout():
        return int(default_value('STOP_TIMEOUT', 60))

    @staticmethod
    def log():
        return default_value('AGENT_LOG_FILE', 'agent.log')

    @staticmethod
    def debug():
        return default_value('DEBUG', 'false') == 'true'

    @staticmethod
    def home():
        return default_value('HOME', '/var/lib/cattle')

    @staticmethod
    def agent_ip():
        return default_value('AGENT_IP', None)

    @staticmethod
    def agent_port():
        return default_value('AGENT_PORT', None)

    @staticmethod
    def config_sh():
        return default_value('CONFIG_SCRIPT',
                             '{0}/config.sh'.format(Config.home()))

    @staticmethod
    def physical_host():
        return {
            'uuid': Config.physical_host_uuid(),
            'type': 'physicalHost',
            'kind': 'physicalHost',
            'name': Config.hostname()
        }

    @staticmethod
    def api_proxy_listen_port():
        return int(default_value('API_PROXY_LISTEN_PORT', '9342'))

    @staticmethod
    def api_proxy_listen_host():
        return default_value('API_PROXY_LISTEN_HOST', '0.0.0.0')

    @staticmethod
    def agent_instance_cattle_home():
        return default_value('AGENT_INSTANCE_CATTLE_HOME', '/var/lib/cattle')

    @staticmethod
    def container_state_dir():
        return path.join(Config.state_dir(), 'containers')

    @staticmethod
    def lock_dir():
        return default_value('LOCK_DIR', os.path.join(Config.home(), 'locks'))

    @staticmethod
    def client_certs_dir():
        client_dir = default_value('CLIENT_CERTS_DIR',
                                   os.path.join(Config.home(), 'client_certs'))
        return client_dir

    @staticmethod
    def stamp():
        return default_value('STAMP_FILE', os.path.join(Config.home(),
                                                        '.pyagent-stamp'))

    @staticmethod
    def config_update_pyagent():
        return default_value('CONFIG_UPDATE_PYAGENT', 'true') == 'true'

    @staticmethod
    def max_dropped_requests():
        return int(default_value('MAX_DROPPED_REQUESTS', '1000'))

    @staticmethod
    def max_dropped_ping():
        return int(default_value('MAX_DROPPED_PING', '10'))

    @staticmethod
    def cadvisor_port():
        return int(default_value('CADVISOR_PORT', '9344'))

    @staticmethod
    def cadvisor_ip():
        return default_value('CADVISOR_IP', '127.0.0.1')

    @staticmethod
    def host_api_ip():
        return default_value('HOST_API_IP', '0.0.0.0')

    @staticmethod
    def host_api_port():
        return int(default_value('HOST_API_PORT', '9345'))

    @staticmethod
    def console_agent_port():
        return int(default_value('CONSOLE_AGENT_PORT', '9346'))

    @staticmethod
    def console_agent_main():
        return default_value('CONSOLE_AGENT_MAIN',
                             os.path.join(Config.home(), 'console-agent',
                                          'agent.js'))

    @staticmethod
    def jwt_public_key_file():
        value = os.path.join(Config.home(), 'etc', 'cattle', 'api.crt')
        return default_value('CONSOLE_HOST_API_PUBLIC_KEY', value)

    @staticmethod
    def host_api_config_file():
        default_path = os.path.join(Config.home(), 'etc', 'cattle',
                                    'host-api.conf')
        return default_value('HOST_API_CONFIG_FILE', default_path)

    @staticmethod
    def host_proxy():
        return default_value('HOST_API_PROXY', None)

    @staticmethod
    def event_read_timeout():
        return int(default_value('EVENT_READ_TIMEOUT', '60'))

    @staticmethod
    def eventlet_backdoor():
        val = default_value('EVENTLET_BACKDOOR', None)
        if val:
            return int(val)
        else:
            return None

    @staticmethod
    def cadvisor_wrapper():
        return default_value('CADVISOR_WRAPPER', '')

    @staticmethod
    def volmgr_storage_driver():
        return default_value('VOLMGR_STORAGE_DRIVER', 'devicemapper')

    @staticmethod
    def volmgr_log_file():
        return default_value('VOLMGR_LOG_FILE', '/var/log/rancher/volmgr.log')

    @staticmethod
    def volmgr_root():
        return default_value('VOLMGR_ROOT', '/var/lib/rancher/volmgr/')

    @staticmethod
    def volmgr_pool_name():
        return default_value('VOLMGR_POOL_NAME', 'rancher-volmgr-pool')

    @staticmethod
    def volmgr_default_volume_size():
        return default_value('VOLMGR_DEFAULT_VOLUME_SIZE', 1073741824)

    @staticmethod
    def volmgr_mount_dir():
        return default_value('VOLMGR_MOUNT_DIR', '/var/lib/rancher/mounts')

    @staticmethod
    def volmgr_dm_data_file():
        return default_value('VOLMGR_DM_DATA_FILE',
                             '/var/lib/rancher/volmgr_data_file')

    @staticmethod
    def volmgr_dm_metadata_file():
        return default_value('VOLMGR_DM_METADATA_FILE',
                             '/var/lib/rancher/volmgr_metadata_file')

    @staticmethod
    def volmgr_dm_data_device():
        return default_value('VOLMGR_DM_DATA_DEVICE', '')

    @staticmethod
    def volmgr_dm_metadata_device():
        return default_value('VOLMGR_DM_METADATA_DEVICE', '')
