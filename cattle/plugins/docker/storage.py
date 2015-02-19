import logging
import os.path
import shutil
from cattle.type_manager import get_type, MARSHALLER
from cattle.storage import BaseStoragePool
from cattle.agent.handler import KindBasedMixin
from . import docker_client, get_compute


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

    @staticmethod
    def _get_image_by_label(uuid):
        parsed_uuid = DockerPool.parse_repo_tag(uuid)
        templates = docker_client().images(all=True, name=parsed_uuid['repo'])
        templates = filter(lambda x: parsed_uuid['uuid'] in x['RepoTags'],
                           templates)
        if len(templates) > 0:
            return templates[0]
        return None

    def pull_image(self, image, progress, auth_config=None):
        if not self._is_image_active(image, None):
            self._do_image_activate(image, None, progress,
                                    auth_config=auth_config)

    def _is_image_active(self, image, storage_pool):
        try:
            try:
                image_obj = self._get_image_by_label(
                    image.data.dockerImage.fullName)
            except AttributeError:
                image_obj = self._get_image_by_label(image.name)
        except AttributeError:
            image_obj = self._get_image_by_label(image)
        return image_obj is not None

    def _do_image_activate(self, image, storage_pool, progress,
                           auth_config=None):
        client = docker_client()
        if isinstance(image, basestring):
            parsed_uuid = DockerPool.parse_repo_tag(image)
        else:
            parsed_uuid = DockerPool.parse_repo_tag(image.uuid)
        repo = parsed_uuid['repo']
        tag = parsed_uuid['tag']
        log.info("Pulling image: [%s]", parsed_uuid['uuid'])
        marshaller = get_type(MARSHALLER)
        if progress is None:
            client.pull(repository=repo, tag=tag, auth_config=auth_config)
        else:
            for status in client.pull(repository=repo, tag=tag, stream=True,
                                      auth_config=auth_config):
                try:
                    log.info('Pulling [%s] status : %s', parsed_uuid['uuid'],
                             status)
                    status = marshaller.from_string(status)
                    message = status['status']
                    progress.update(message)
                except:
                    pass

    def _get_image_storage_pool_map_data(self, obj):
        try:
            image = self._get_image_by_label(
                obj.image.data.dockerImage.fullName)
        except (KeyError, AttributeError):
            image = self._get_image_by_label(obj.image.name)
        return {
            '+data': {
                'dockerImage': image
            }
        }

    def _get_volume_storage_pool_map_data(self, obj):
        return {
            'volume': {
                'format': 'docker'
            }
        }

    def _is_volume_active(self, volume, storage_pool):
        return True

    def _is_volume_inactive(self, volume, storage_pool):
        return True

    def _is_volume_removed(self, volume, storage_pool):
        if volume.deviceNumber == 0:
            container = get_compute().get_container_by_name(
                volume.instance.uuid)
            return container is None
        else:
            if volume.data.fields['isHostPath']:
                # If this is a host path volume, we'll never really remove it
                # from disk, so just report is as removed for the purpose of
                # handling the event.
                return True

            path = self._path_to_volume(volume)
            return not os.path.exists(path)

    def _do_volume_remove(self, volume, storage_pool, progress):
        if volume.deviceNumber == 0:
            container = get_compute().get_container_by_name(
                volume.instance.uuid)
            if container is None:
                return
            docker_client().remove_container(container)
        else:
            if not volume.data.fields['isHostPath']:
                path = self._path_to_volume(volume)
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
            return {'repo': image_uuid, 'tag': '', 'uuid': image_uuid}
        tag = image_uuid[n+1:]
        if tag.find("/") < 0:
            return {'repo': image_uuid[:n], 'tag': tag, 'uuid': image_uuid}
        return {'repo': image_uuid, 'tag': '', 'uuid': image_uuid}
