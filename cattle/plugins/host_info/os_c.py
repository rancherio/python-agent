import platform
import cattle.utils


class OSCollector(object):
    def key_name(self):
        return "osInfo"

    def _zip_fields_values(self, keys, values):
        data = {}
        for key, value in zip(keys, values):
            if len(value) > 0:
                data[key] = value
            else:
                data[key] = None

        return data

    def _get_docker_version(self):
        data = {}

        if platform.system() == 'Linux':
            data['dockerVersion'] = \
                cattle.utils.check_output(['docker', '-v']).rstrip()

        return data

    def _get_os(self):
        data = {}
        if platform.system() == 'Linux':
            info = platform.linux_distribution()
            keys = ["distribution", "version", "versionDescription"]
            data = self._zip_fields_values(keys, info)

            data['kernelVersion'] = \
                platform.release() if len(platform.release()) > 0 else None

        return data

    def get_data(self):
        data = self._get_os()
        data.update(self._get_docker_version())

        return data
