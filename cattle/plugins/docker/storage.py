import logging
from cattle.plugins.volmgr import volmgr
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
    def _get_image_by_label(tag):
        parsed_tag = DockerPool.parse_repo_tag(tag)
        templates = docker_client().images(all=True, name=parsed_tag['repo'])
        templates = filter(lambda x: parsed_tag['uuid'] in x['RepoTags'],
                           templates)

        if len(templates) > 0:
            return templates[0]
        return None

    def pull_image(self, image, progress):
        if not self._is_image_active(image, None):
            self._do_image_activate(image, None, progress)

    def _is_image_active(self, image, storage_pool):
        image_obj = self._get_image_by_label(image.data.dockerImage.fullName)
        return image_obj is not None

    def _do_image_activate(self, image, storage_pool, progress):
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
        if progress is None:
            result = client.pull(repository=data.qualifiedName,
                                 tag=data.tag, auth_config=auth_config)
            if 'error' in result:
                raise ImageValidationError('Image [%s] failed to pull' %
                                           data.fullName)
        else:
            for status in client.pull(repository=data.qualifiedName,
                                      tag=data.tag,
                                      auth_config=auth_config,
                                      stream=True):
                log.info('Pulling [%s] status : %s', data.fullName, status)
                status = marshaller.from_string(status)
                try:
                    message = status['status']
                except KeyError:
                    message = status['error']
                    raise ImageValidationError('Image [%s] failed to pull '
                                               ': %s' % (data.fullName,
                                                         message))
                progress.update(message)

    def _get_image_storage_pool_map_data(self, obj):
        image = self._get_image_by_label(obj.image.data.dockerImage.fullName)
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
            container = get_compute().get_container(docker_client(),
                                                    volume.instance)
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
            container = get_compute().get_container(docker_client(),
                                                    volume.instance)
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
            return {'repo': image_uuid,
                    'tag': 'latest',
                    'uuid': image_uuid + ':latest'}
        tag = image_uuid[n+1:]
        if tag.find("/") < 0:
            return {'repo': image_uuid[:n], 'tag': tag, 'uuid': image_uuid}
        return {'repo': image_uuid,
                'tag': 'latest',
                'uuid': image_uuid + ':latest'}

    def _is_snapshot_created(self, snapshot, storage_pool):
        log.info("Check snapshot created")
        return 'snapshotUUID' in snapshot.data.fields

    def _is_snapshot_removed(self, snapshot, storage_pool):
        log.info("Check snapshot removed")
        return 'removed' in snapshot.data.fields

    def _do_snapshot_create(self, snapshot, storage_pool, progress):
        log.info("Creating snapshot")
        volume_uuid = snapshot.data.fields['volumeUUID']
        log.info("Creating snapshot for volume %s" % volume_uuid)
        snapshot_uuid = volmgr.create_snapshot(volume_uuid)
        snapshot.data.fields['snapshotUUID'] = snapshot_uuid
        log.info("New snapshot for volume, uuid is %s" % snapshot_uuid)
        volmgr.backup_snapshot(snapshot_uuid, volume_uuid)
        log.info("Backup snapshot for volume, uuid is %s" % snapshot_uuid)
        return True

    def _do_snapshot_remove(self, snapshot, storage_pool, progress):
        volume_uuid = snapshot.data.fields['volumeUUID']
        snapshot_uuid = snapshot.data.fields['snapshotUUID']
        log.info("Removing snapshot %s for volume %s" % (
            snapshot_uuid, volume_uuid))
        volmgr.remove_snapshot_from_blockstore(snapshot_uuid, volume_uuid)
        volmgr.delete_snapshot(snapshot_uuid, volume_uuid)
        log.info("Removed snapshot %s for volume %s" % (
                 snapshot_uuid, volume_uuid))
        snapshot.data.fields['removed'] = True
        return True

    def _get_snapshot_storage_pool_map_data(self, obj):
        snapshot = obj.snapshot
        return {
            'snapshot': {
                '+data': {
                    '+fields': {
                        'snapshotUUID': snapshot.data.fields['snapshotUUID'],
                    }
                }
            }
        }


class ImageValidationError(Exception):
    pass


class AuthConfigurationError(Exception):
    pass
