import os
from cattle.plugins.docker import DockerConfig
from cattle import Config

from cattle.process_manager import background


class HostApi(object):

    def on_startup(self):
        env = dict(os.environ)
        env['HOST_API_CONFIG_FILE'] = host_api_config()

        url = 'http://{0}:{1}'.format(Config.cadvisor_ip(),
                                      Config.cadvisor_port())

        background(['host-api',
                    '-cadvisor-url',  url,
                    '-logtostderr=true',
                    '-ip', Config.host_api_ip(),
                    '-port', str(Config.host_api_port()),
                    '-auth=true',
                    '-host-uuid', DockerConfig.docker_uuid(),
                    '-public-key', Config.jwt_public_key_file(),
                    '-cattle-url', Config.api_url(),
                    '-cattle-access-key', Config.access_key(),
                    '-cattle-secret-key', Config.secret_key(),
                    '-cattle-state-dir', Config.container_state_dir()],
                   env=env)


def host_api_config():
    return os.path.join(os.path.dirname(__file__), 'host-api.conf')
