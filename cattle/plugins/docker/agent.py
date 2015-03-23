from cattle import Config
from cattle.plugins.docker.util import add_to_env
from urlparse import urlparse


def setup_cattle_config_url(instance, create_config):
    if instance.get('agentId') is None:
        return

    url = Config.config_url()

    if url is not None:
        parsed = urlparse(url)

        if 'localhost' == parsed.hostname:
            port = Config.api_proxy_listen_port()
            add_to_env(create_config,
                       CATTLE_AGENT_INSTANCE='true',
                       CATTLE_CONFIG_URL_SCHEME=parsed.scheme,
                       CATTLE_CONFIG_URL_PATH=parsed.path,
                       CATTLE_CONFIG_URL_PORT=port)
        else:
            add_to_env(create_config, CATTLE_CONFIG_URL=url)
