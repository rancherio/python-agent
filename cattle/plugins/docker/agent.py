from cattle import Config
from cattle.plugins.docker.util import add_to_env
from urlparse import urlparse


def _has_label(instance):
    try:
        return instance.labels['io.rancher.container.cattle_url'] == 'true'
    except:
        pass
    return False


def setup_cattle_config_url(instance, create_config):
    if instance.get('agentId') is None and not _has_label(instance):
        return

    if 'labels' not in create_config:
        create_config['labels'] = {}

    create_config['labels']['io.rancher.container.agent_id'] = \
        str(instance.get('agentId'))

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
            add_to_env(create_config, CATTLE_URL=url)
