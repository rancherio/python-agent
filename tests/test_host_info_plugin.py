import pytest
import os
import tests
import platform
import json
import cattle.utils

from cattle.plugins.host_info.main import HostInfo
from cattle.plugins.host_info.cpu import CpuCollector
from cattle.plugins.host_info.memory import MemoryCollector
from cattle.utils import CadvisorAPIClient

TEST_DIR = os.path.join(os.path.dirname(tests.__file__))


def cpuinfo_data():
    with open(os.path.join(TEST_DIR, 'host_info/cpuinfo')) as mf:
        return mf.readlines()


def meminfo_data():
    with open(os.path.join(TEST_DIR, 'host_info/meminfo')) as mf:
        return mf.readlines()


def cadvisor_stats_data():
    with open(os.path.join(TEST_DIR, 'host_info/cadvisor_stats')) as mf:
        return json.loads(mf.read())


@pytest.fixture()
def host_data(mocker):
    mocker.patch.object(platform, 'system', return_value='Linux')
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

    mocker.patch('cattle.utils.check_output',
                 return_value='Docker version 1.4.1, build 5bc2ff8')

    host = HostInfo()
    data = host.collect_data()

    assert isinstance(data, dict)
    os.getloadavg.assert_called_once_with()
    CpuCollector._get_cpuinfo_data.assert_called_once_with()
    MemoryCollector._get_meminfo_data.assert_called_once_with()
    CadvisorAPIClient.get_containers.assert_called_with()
    cattle.utils.check_output.assert_called_once_with(
        ['docker', '-v'])

    return data


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

    mocker.patch('cattle.utils.check_output',
                 return_value='Docker version 1.4.1, build 5bc2ff8')

    host = HostInfo()
    data = host.collect_data()

    return data


@pytest.fixture()
def host_data_non_linux(mocker):
    mocker.patch.object(platform, 'system', return_value='notLinux')
    return HostInfo().collect_data()


def test_collect_data(host_data):
    expected_top_keys = ['memoryInfo', 'osInfo', 'cpuInfo', 'diskInfo']

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


def test_collect_data_osinfo(host_data):
    expected_osinfo_keys = ['distribution',
                            'dockerVersion',
                            'version',
                            'versionDescription',
                            'kernelVersion']

    assert sorted(host_data['osInfo'].keys()) == \
        sorted(expected_osinfo_keys)

    assert host_data['osInfo']['dockerVersion'] == \
        'Docker version 1.4.1, build 5bc2ff8'


def test_collect_data_diskinf(host_data):
    expected_diskinfo_keys = ['mountPoints']

    assert sorted(host_data['diskInfo']) == sorted(expected_diskinfo_keys)
    assert host_data['diskInfo']['mountPoints'].keys() == ['/dev/sda1']
    mount_point = host_data['diskInfo']['mountPoints']

    assert mount_point['/dev/sda1']['percentUsed'] == 24.15
    assert mount_point['/dev/sda1']['total'] == 28447.621
    assert mount_point['/dev/sda1']['used'] == 6869.797
    assert mount_point['/dev/sda1']['free'] == 21577.824


def test_collect_data_bad_cadvisor_stat(no_cadvisor_host_data):
    expected_cpuinfo_keys = ['modelName',
                             'count',
                             'mhz',
                             'loadAvg',
                             'cpuCoresPercentages'
                             ]
    expected_disk_info = {'mountPoints': {}}

    assert sorted(no_cadvisor_host_data['cpuInfo']) == \
        sorted(expected_cpuinfo_keys)
    assert no_cadvisor_host_data['cpuInfo']['cpuCoresPercentages'] == []

    assert no_cadvisor_host_data['diskInfo'] == expected_disk_info


def test_collect_data_cpuinfo(host_data):
    expected_cpuinfo_keys = ['modelName',
                             'count',
                             'mhz',
                             'loadAvg',
                             'cpuCoresPercentages'
                             ]

    expected_core_usages = [80.135, 100.0, 35.338, 28.129]

    assert sorted(host_data['cpuInfo']) == sorted(expected_cpuinfo_keys)
    assert host_data['cpuInfo']['loadAvg'] == \
        [1.60693359375, 1.73193359375, 1.79248046875]

    assert host_data['cpuInfo']['modelName'] == \
        "Intel(R) Core(TM) i7-4650U CPU @ 1.70GHz"

    assert host_data['cpuInfo']['mhz'] == 2334.915
    assert host_data['cpuInfo']['count'] == 4

    assert host_data['cpuInfo']['cpuCoresPercentages'] == expected_core_usages


def test_non_linux_host(host_data_non_linux):
    expected_top_keys = ['memoryInfo', 'osInfo', 'cpuInfo', 'diskInfo']
    expected_empty = {}

    assert sorted(host_data_non_linux.keys()) == sorted(expected_top_keys)
    assert host_data_non_linux['memoryInfo'] == expected_empty
    assert host_data_non_linux['osInfo'] == expected_empty
    assert host_data_non_linux['cpuInfo'] == expected_empty
    assert 'mountPoints' in host_data_non_linux['diskInfo'].keys()
