import pytest
import os
import tests
import platform
import json

from cattle.plugins.host_info.main import HostInfo
from cattle.plugins.host_info.cpu import CpuCollector
from cattle.plugins.host_info.memory import MemoryCollector
from cattle.plugins.docker import docker_client
from cattle.utils import CadvisorAPIClient
from docker.client import Client

from .common_fixtures import *  # NOQA
from .docker_common import *  # NOQA

TEST_DIR = os.path.join(os.path.dirname(tests.__file__))


def cpuinfo_data():
    with open(os.path.join(TEST_DIR, 'host_info/cpuinfo')) as mf:
        return mf.readlines()


def non_intel_cpuinfo_data():
    d = cpuinfo_data()

    for line in d:
        if line.startswith("model name"):
            d[d.index(line)] = "model name : AMD Opteron 250\n"

    return d


def meminfo_data():
    with open(os.path.join(TEST_DIR, 'host_info/meminfo')) as mf:
        return mf.readlines()


def cadvisor_stats_data():
    with open(os.path.join(TEST_DIR, 'host_info/cadvisor_stats')) as mf:
        return json.loads(mf.read())


def cadvisor_machine_stats_data():
    with open(os.path.join(TEST_DIR, 'host_info/cadvisor_machine')) as mf:
        return json.loads(mf.read())


def docker_devicemapper_override():
    data = docker_client().info()
    data['Driver'] = "devicemapper"
    data['DriverStatus'] = \
        [['Pool Name', 'docker-8:1-130861-pool'],
         ['Pool Blocksize', '65.54 kB'], ['Backing Filesystem', 'extfs'],
         ['Data file', '/dev/loop0'], ['Metadata file', '/dev/loop1'],
         ['Data Space Used', '2.661 GB'], ['Data Space Total', '107.4 GB'],
         ['Data Space Available', '16.8 GB'],
         ['Metadata Space Used', '2.683 MB'],
         ['Metadata Space Total', '2.147 GB'],
         ['Metadata Space Available', '2.145 GB'],
         ['Udev Sync Supported', 'false'],
         ['Deferred Removal Enabled', 'false'],
         ['Data loop file',
             '/mnt/sda1/var/lib/docker/devicemapper/devicemapper/data'],
         ['Metadata loop file',
             '/mnt/sda1/var/lib/docker/devicemapper/devicemapper/metadata'],
         ['Library Version', '1.02.82-git (2013-10-04)']]
    return data


def docker_client_version_data():
    return json.loads('{"KernelVersion": "4.0.3-boot2docker", "Arch": "amd64",'
                      '"ApiVersion": "1.18", "Version": "1.6.0", "GitCommit": '
                      '"4749651", "Os": "linux", "GoVersion": "go1.4.2"}')


@pytest.fixture
def full_mock_hostinfo_obj(mocker):
    mocker.patch.object(platform, 'system', return_value='Linux')
    mocker.patch.object(platform, 'release', return_value='3.19.0-28-generic')

    mocker.patch('os.getloadavg',
                 return_value=(1.60693359375, 1.73193359375, 1.79248046875))

    mocker.patch.object(CpuCollector,
                        '_get_cpuinfo_data',
                        return_value=cpuinfo_data())

    mocker.patch.object(MemoryCollector,
                        '_get_meminfo_data',
                        return_value=meminfo_data())

    mocker.patch.object(CadvisorAPIClient, 'get_containers',
                        return_value=cadvisor_stats_data())

    mocker.patch.object(CadvisorAPIClient, 'get_machine_stats',
                        return_value=cadvisor_machine_stats_data())

    mocker.patch.object(Client, 'version',
                        return_value=docker_client_version_data())

    mocker.patch.object(Client, 'info',
                        return_value=docker_devicemapper_override())

    return HostInfo(docker_client())


@pytest.fixture()
def host_data(full_mock_hostinfo_obj):
    data = full_mock_hostinfo_obj.collect_data()

    assert isinstance(data, dict)
    os.getloadavg.assert_called_once_with()
    CpuCollector._get_cpuinfo_data.assert_called_once_with()
    MemoryCollector._get_meminfo_data.assert_called_once_with()
    CadvisorAPIClient.get_containers.assert_called_with()
    CadvisorAPIClient.get_machine_stats.assert_called_with()
    Client.version.assert_called_once_with()

    return data


@pytest.fixture()
def host_labels(full_mock_hostinfo_obj):
    return full_mock_hostinfo_obj.host_labels()


@pytest.fixture()
def no_cadvisor_host_data(mocker):
    mocker.patch.object(platform, 'system', return_value='Linux')
    mocker.patch('os.getloadavg',
                 return_value=(1.60693359375, 1.73193359375, 1.79248046875))

    mocker.patch.object(CpuCollector,
                        '_get_cpuinfo_data',
                        return_value=cpuinfo_data())

    mocker.patch.object(MemoryCollector,
                        '_get_meminfo_data',
                        return_value=meminfo_data())

    mocker.patch.object(CadvisorAPIClient, '_get',
                        return_value=None)

    host = HostInfo()
    data = host.collect_data()

    return data


@pytest.fixture()
def no_cadvisor_non_intel_cpuinfo_mock(mocker):
    mocker.patch.object(platform, 'system', return_value='Linux')
    mocker.patch('os.getloadavg',
                 return_value=(1.60693359375, 1.73193359375, 1.79248046875))

    mocker.patch.object(CpuCollector,
                        '_get_cpuinfo_data',
                        return_value=non_intel_cpuinfo_data())

    mocker.patch.object(MemoryCollector,
                        '_get_meminfo_data',
                        return_value=meminfo_data())

    mocker.patch.object(CadvisorAPIClient, '_get',
                        return_value=None)

    host = HostInfo()
    data = host.collect_data()

    return data


@pytest.fixture()
def host_data_non_linux(mocker):
    mocker.patch.object(platform, 'system', return_value='notLinux')
    return HostInfo().collect_data()


def test_hostlabels(host_labels):
    expected = {
        'io.rancher.host.docker_version': '1.6',
        'io.rancher.host.linux_kernel_version': '3.19'
    }

    if 'io.rancher.host.kvm' in host_labels:
        del host_labels['io.rancher.host.kvm']
    assert host_labels == expected


def test_collect_data(host_data):
    expected_top_keys = ['memoryInfo', 'osInfo', 'cpuInfo', 'diskInfo',
                         'iopsInfo']

    assert sorted(host_data.keys()) == sorted(expected_top_keys)


def test_collect_data_meminfo(host_data):
    expected_meminfo_keys = ['memTotal',
                             'memFree',
                             'memAvailable',
                             'buffers',
                             'cached',
                             'swapCached',
                             'active',
                             'inactive',
                             'swapTotal',
                             'swapFree'
                             ]

    assert sorted(host_data['memoryInfo'].keys()) == \
        sorted(expected_meminfo_keys)

    assert host_data['memoryInfo']['memTotal'] == 3037.414


@if_docker
def test_collect_data_osinfo(host_data):
    expected_osinfo_keys = ['operatingSystem',
                            'dockerVersion',
                            'kernelVersion']

    assert sorted(host_data['osInfo'].keys()) == \
        sorted(expected_osinfo_keys)

    assert host_data['osInfo']['dockerVersion'] == \
        'Docker version 1.6.0, build 4749651'

    operating_system = docker_client().info().get("OperatingSystem")
    assert host_data['osInfo']['operatingSystem'] == operating_system


def test_collect_data_diskinf(host_data):
    expected_diskinfo_keys = [
        'fileSystems', 'mountPoints', 'dockerStorageDriver',
        'dockerStorageDriverStatus']

    assert sorted(host_data['diskInfo']) == sorted(expected_diskinfo_keys)
    assert host_data['diskInfo']['mountPoints'].keys() == ['/dev/sda1']

    assert host_data['diskInfo']['fileSystems'].keys() > 0
    assert not ("/dev/mapper/docker-8:1-130861-c3ae1852921c3fec9c9a74dce987f"
                "47f7e1ae8e7e3bcd9ad98e671f5d80a28d8") in \
        host_data['diskInfo']['fileSystems']


def test_collect_data_bad_cadvisor_stat(no_cadvisor_host_data):
    expected_cpuinfo_keys = ['modelName',
                             'count',
                             'mhz',
                             'loadAvg',
                             'cpuCoresPercentages'
                             ]

    expected_disk_info_keys = ['mountPoints',
                               'fileSystems',
                               'dockerStorageDriverStatus',
                               'dockerStorageDriver']

    assert sorted(no_cadvisor_host_data['cpuInfo']) == \
        sorted(expected_cpuinfo_keys)
    assert no_cadvisor_host_data['cpuInfo']['cpuCoresPercentages'] == []

    assert sorted(no_cadvisor_host_data['diskInfo'].keys()) == \
        sorted(expected_disk_info_keys)


def test_collect_data_cpuinfo(host_data):
    expected_cpuinfo_keys = ['modelName',
                             'count',
                             'mhz',
                             'loadAvg',
                             'cpuCoresPercentages'
                             ]

    assert sorted(host_data['cpuInfo']) == sorted(expected_cpuinfo_keys)

    assert host_data['cpuInfo']['modelName'] == \
        "Intel(R) Core(TM) i7-4650U CPU @ 1.70GHz"

    assert host_data['cpuInfo']['mhz'] == 1700


def test_collect_data_cpu_freq_fallback(no_cadvisor_non_intel_cpuinfo_mock):
    assert no_cadvisor_non_intel_cpuinfo_mock['cpuInfo']['mhz'] == 2334.915


def test_non_linux_host(host_data_non_linux):
    expected_top_keys = ['memoryInfo', 'osInfo', 'cpuInfo', 'diskInfo',
                         'iopsInfo']
    expected_empty = {}

    assert sorted(host_data_non_linux.keys()) == sorted(expected_top_keys)
    assert host_data_non_linux['memoryInfo'] == expected_empty
    assert host_data_non_linux['osInfo'] == expected_empty
    assert host_data_non_linux['cpuInfo'] == expected_empty

    assert 'mountPoints' in host_data_non_linux['diskInfo'].keys()
    assert 'fileSystems' in host_data_non_linux['diskInfo'].keys()
    assert 'dockerStorageDriver' in host_data_non_linux['diskInfo'].keys()
    assert 'dockerStorageDriverStatus' in \
        host_data_non_linux['diskInfo'].keys()
