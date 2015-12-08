import logging
import os.path
import shutil
import requests
from contextlib import closing
from cattle.type_manager import get_type, MARSHALLER
from cattle.storage import BaseStoragePool
from cattle.agent.handler import KindBasedMixin
from cattle.plugins.docker.util import is_no_op, remove_container
from cattle.lock import lock
from cattle.progress import Progress
from . import docker_client, get_compute, DockerConfig
from docker.errors import APIError
from cattle.utils import is_str_set, JsonObject

log = logging.getLogger('docker')


class DockerPool(KindBasedMixin, BaseStoragePool):
    def __init__(self):
        KindBasedMixin.__init__(self, kind='docker')
        BaseStoragePool.__init__(self)

    @staticmethod
    def _get_image_by_id(id):
        templates = docker_client().images(all=True)
        templates = filter(lambda x: x['Id'] == id, templates)

        if len(templates) > 0:
            return templates[0]
        return None

    def pull_image(self, image, progress):
        if not self._is_image_active(image, None):
            self._do_image_activate(image, None, progress)

    def _is_image_active(self, image, storage_pool):
        if is_no_op(image):
            return True
        parsed_tag = DockerPool.parse_repo_tag(image.data.dockerImage.fullName)
        try:
            if len(docker_client().inspect_image(parsed_tag['uuid'])):
                return True
        except APIError:
            pass
        return False

    def _image_build(self, image, progress):
        client = docker_client()
        opts = dict(image.data.fields.build)

        def do_build():
            for key in ['context', 'remote']:
                if key in opts:
                    del opts[key]
            opts['stream'] = True
            marshaller = get_type(MARSHALLER)
            for status in client.build(**opts):
                try:
                    status = marshaller.from_string(status)
                    progress.update(status['stream'])
                except:
                    pass

        if is_str_set(opts, 'context'):
            with closing(requests.get(opts['context'], stream=True)) as r:
                if r.status_code != 200:
                    raise Exception('Bad response {} from {}'
                                    .format(r.status_code,
                                            opts['context']))
                del opts['context']
                opts['fileobj'] = ResponseWrapper(r)
                opts['custom_context'] = True
                do_build()
        else:
            remote = opts['remote']
            if remote.startswith('git@github.com:'):
                remote = remote.replace('git@github.com:', 'git://github.com/')
            del opts['remote']
            opts['path'] = remote
            do_build()

    def _is_build(self, image):
        try:
            if is_str_set(image.data.fields.build, 'context') or \
                    is_str_set(image.data.fields.build, 'remote'):
                return True
        except (KeyError, AttributeError):
            pass

        return False

    @staticmethod
    def image_pull(image, progress):
        return DockerPool()._do_image_activate(image, None, progress)

    def _do_image_activate(self, image, storage_pool, progress):
        if is_no_op(image):
            return

        if self._is_build(image):
            return self._image_build(image, progress)

        auth_config = None
        try:
            if 'registryCredential' in image:
                if image.registryCredential is not None:
                    auth_config = {
                        'username': image.registryCredential['publicValue'],
                        'email': image.registryCredential['data']['fields']
                        ['email'],
                        'password': image.registryCredential['secretValue'],
                        'serveraddress': image.registryCredential['registry']
                        ['data']['fields']['serverAddress']
                    }
                    if auth_config['serveraddress'] == "https://docker.io":
                        auth_config['serveraddress'] =\
                            "https://index.docker.io"
                    log.debug('Auth_Config: [%s]', auth_config)
            else:
                log.debug('No Registry credential found. Pulling non-authed')
        except (AttributeError, KeyError, TypeError) as e:
            raise AuthConfigurationError("Malformed Auth Config. \n\n"
                                         "error: [%s]\nregistryCredential:"
                                         " %s"
                                         % (e, image.registryCredential))
        client = docker_client()
        data = image.data.dockerImage
        marshaller = get_type(MARSHALLER)
        temp = data.qualifiedName
        if data.qualifiedName.startswith('docker.io/'):
            temp = 'index.' + data.qualifiedName
        # Always pass insecure_registry=True to prevent docker-py
        # from pre-verifying the registry. Let the docker daemon handle
        # the verification of and connection to the registry.
        if progress is None:
            result = client.pull(repository=temp,
                                 tag=data.tag, auth_config=auth_config,
                                 insecure_registry=True)
            if 'error' in result:
                raise ImageValidationError('Image [%s] failed to pull: %s' %
                                           (data.fullName, result['error']))
        else:
            last_message = ''
            message = ''
            for status in client.pull(repository=temp,
                                      tag=data.tag,
                                      auth_config=auth_config,
                                      stream=True,
                                      insecure_registry=True):
                try:
                    status = marshaller.from_string(status)
                    if 'error' in status:
                        message = status['error']
                        raise ImageValidationError('Image [%s] failed to pull:'
                                                   ' %s' % (data.fullName,
                                                            message))
                    if 'status' in status:
                        message = status['status']
                except:
                    # Ignore errors reading the status from Docker
                    continue

                if last_message != message:
                    progress.update(message)
                    last_message = message

    def _get_image_storage_pool_map_data(self, obj):
        return {}

    def _get_volume_storage_pool_map_data(self, obj):
        return {
            'volume': {
                'format': 'docker'
            }
        }

    @staticmethod
    def is_volume_active(volume):
        return DockerPool()._is_volume_active(volume, None)

    def _is_volume_active(self, volume, storage_pool):
        if not self._is_managed_volume(volume):
            return True
        try:
            v = DockerConfig.storage_api_version()
            docker_client(version=v).inspect_volume(volume.name)
        except APIError:
            return False
        return True

    @staticmethod
    def do_volume_activate(volume):
        return DockerPool()._do_volume_activate(volume, None, None)

    def _do_volume_activate(self, volume, storage_pool, progress):
        if not self._is_managed_volume(volume):
            return
        driver = volume.data['fields']['driver']
        try:
            if volume.data['fields']['driverOpts']:
                driver_opts = volume.data['fields']['driverOpts']
            else:
                driver_opts = None
        except KeyError:
            driver_opts = None
        v = DockerConfig.storage_api_version()
        docker_client(version=v).create_volume(volume.name,
                                               driver,
                                               JsonObject.unwrap(driver_opts))

    def _is_volume_inactive(self, volume, storage_pool):
        return True

    def _is_volume_removed(self, volume, storage_pool):
        if volume.deviceNumber == 0:
            container = get_compute().get_container(docker_client(),
                                                    volume.instance)
            return container is None
        elif self._is_managed_volume(volume):
            return not self._is_volume_active(volume, storage_pool)
        else:
            path = self._path_to_volume(volume)
            if volume.data.fields['isHostPath']:
                # If this is a host path volume, we'll never really remove it
                # from disk, so just report is as removed for the purpose of
                # handling the event.
                return True

            return not os.path.exists(path)

    def _is_managed_volume(self, volume):
        try:
            driver = volume.data['fields']['driver']
            if driver is None or driver == "":
                return False
            if volume.name is None or volume.name == "":
                return False
        except (AttributeError, KeyError):
            return False
        return True

    def _do_volume_remove(self, volume, storage_pool, progress):
        if volume.deviceNumber == 0:
            container = get_compute().get_container(docker_client(),
                                                    volume.instance)
            if container is None:
                return
            remove_container(docker_client(), container)
        elif self._is_managed_volume(volume):
            v = DockerConfig.storage_api_version()
            docker_client(version=v).remove_volume(volume.name)
        else:
            path = self._path_to_volume(volume)
            if not volume.data.fields['isHostPath']:
                if os.path.exists(path):
                    log.info("Deleting volume: %s" % volume.uri)
                    shutil.rmtree(path)

    def _path_to_volume(self, volume):
        return volume.uri.replace('file://', '')

    @staticmethod
    def parse_repo_tag(image_uuid):
        if image_uuid.startswith('docker:'):
                    image_uuid = image_uuid[7:]
        n = image_uuid.rfind(":")
        if n < 0:
            return {'repo': image_uuid,
                    'tag': 'latest',
                    'uuid': image_uuid + ':latest'}
        tag = image_uuid[n+1:]
        if tag.find("/") < 0:
            return {'repo': image_uuid[:n], 'tag': tag, 'uuid': image_uuid}
        return {'repo': image_uuid,
                'tag': 'latest',
                'uuid': image_uuid + ':latest'}

    def volume_remove(self, req=None, volumeStoragePoolMap=None, **kw):
        volume = volumeStoragePoolMap.volume
        storage_pool = volumeStoragePoolMap.storagePool
        progress = Progress(req)

        with lock(volume):
            if volume.deviceNumber == 0:
                get_compute().purge_state(docker_client(), volume.instance)

            if not self._is_volume_removed(volume, storage_pool):
                self._do_volume_remove(volume, storage_pool, progress)

            data = self._get_response_data(req, volumeStoragePoolMap)
            return self._reply(req, data)


class ImageValidationError(Exception):
    pass


class AuthConfigurationError(Exception):
    pass


class ResponseWrapper(object):
    """"
    This wrapper is to prevent requests from incorrectly setting the
    Content-Length on the request.  If you do not use this wrapper requests
    finds r.raw.fileno and uses the size of that FD, which is 0
    """
    def __init__(self, response):
        self.r = response

    def __iter__(self):
        return self.r.raw.__iter__()
