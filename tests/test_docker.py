import time
from cattle.plugins.docker.network import setup_mac_and_ip
from cattle.plugins.host_info.main import HostInfo
from cattle.plugins.docker.util import remove_container
from cattle.plugins.docker.compute import DockerCompute
from cattle.plugins.docker import DockerConfig

from .common_fixtures import *  # NOQA
from .docker_common import *  # NOQA
from docker.errors import APIError


@pytest.fixture(scope='module')
def pull_images():
    client = docker_client()
    images = [('ibuildthecloud/helloworld', 'latest'),
              ('rancher/agent', 'v0.7.9'),
              ('rancher/agent', 'latest')]
    for i in images:
        try:
            client.inspect_image(':'.join(i))
        except APIError:
            client.pull(i[0], i[1])


@if_docker
def test_volume_activate(agent, responses):
    event_test(agent, 'docker/volume_activate')


@if_docker
def test_volume_activate_driver1(agent, responses):
    def pre(req):
        vol = req['data']['volumeStoragePoolMap']['volume']
        vol['data'] = {'fields': {'driver': 'local',
                                  'driverOpts': None}}
        vol['name'] = 'test_vol'

    def post(req, resp):
        v = DockerConfig.storage_api_version()
        vol = docker_client(version=v).inspect_volume('test_vol')
        assert vol['Driver'] == 'local'
        assert vol['Name'] == 'test_vol'
        docker_client(version=v).remove_volume('test_vol')

    event_test(agent, 'docker/volume_activate', pre_func=pre, post_func=post)


@if_docker
def test_volume_activate_driver2(agent, responses):
    def pre(req):
        vol = req['data']['volumeStoragePoolMap']['volume']
        vol['data'] = {'fields': {'driver': 'local',
                                  'driverOpts': {'size': '10G'}}}
        vol['name'] = 'test_vol'

    def post(req, resp):
        v = DockerConfig.storage_api_version()
        vol = docker_client(version=v).inspect_volume('test_vol')
        assert vol['Driver'] == 'local'
        assert vol['Name'] == 'test_vol'
        docker_client(version=v).remove_volume('test_vol')

    event_test(agent, 'docker/volume_activate', pre_func=pre, post_func=post)


@if_docker
def test_volume_deactivate_driver(agent, responses):
    def pre(req):
        v = DockerConfig.storage_api_version()
        docker_client(version=v).create_volume('test_vol',
                                               'local')
        vol = req['data']['volumeStoragePoolMap']['volume']
        vol['data'] = {'fields': {'driver': 'local',
                                  'driverOpts': {'size': '10G'}}}
        vol['name'] = 'test_vol'

    def post(req, resp):
        v = DockerConfig.storage_api_version()
        vol = docker_client(version=v).inspect_volume('test_vol')
        assert vol['Driver'] == 'local'
        assert vol['Name'] == 'test_vol'
        docker_client(version=v).remove_volume('test_vol')

    event_test(agent, 'docker/volume_deactivate', pre_func=pre, post_func=post)


@if_docker
def test_volume_deactivate(agent, responses):
    event_test(agent, 'docker/volume_deactivate')


@if_docker
def test_instance_activate_volume_driver(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['volumeDriver'] = 'local'

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        if newer_than('1.19'):
            if newer_than('1.21'):
                assert docker_inspect['HostConfig']['VolumeDriver'] == 'local'
            else:
                assert docker_inspect['Config']['VolumeDriver'] == 'local'
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate', pre_func=pre, post_func=post)


@if_docker
def test_volume_remove_driver(agent, responses):
    def pre(req):
        v = DockerConfig.storage_api_version()
        docker_client(version=v).create_volume('test_vol',
                                               'local')
        vol = req['data']['volumeStoragePoolMap']['volume']
        vol['data'] = {'fields': {'driver': 'local',
                                  'driverOpts': {'size': '10G'}}}
        vol['name'] = 'test_vol'
        vol['uri'] = 'local:///test_vol'

    def post(req, resp):
        v = DockerConfig.storage_api_version()
        with pytest.raises(APIError) as e:
            docker_client(version=v).inspect_volume('test_vol')
        assert e.value.explanation == 'no such volume'

    event_test(agent, 'docker/volume_remove', pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_no_mac_address(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        for nic in instance['nics']:
            nic['macAddress'] = ''

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        mac_received = docker_inspect['Config']['MacAddress']
        mac_nic_received = docker_inspect['NetworkSettings']['MacAddress']
        assert mac_received == ''
        assert mac_nic_received is not None
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate', pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_mac_address(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        mac_received = docker_inspect['Config']['MacAddress']
        mac_nic_received = docker_inspect['NetworkSettings']['MacAddress']
        assert mac_nic_received == '02:03:04:05:06:07'
        assert mac_received == '02:03:04:05:06:07'
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate', post_func=post)


def test_multiple_nics_pick_mac():
    instance = {
        'nics': [
            {
                'macAddress': '02:03:04:05:06:07',
                'deviceNumber': 0
            },
            {
                'macAddress': '02:03:04:05:06:09',
                'deviceNumber': 1
            }
        ]
    }
    instance = JsonObject(instance)
    config = {'test': 'Nothing'}
    setup_mac_and_ip(instance, config)
    assert config['mac_address'] == '02:03:04:05:06:07'


@if_docker
def test_instance_activate_ports(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        del instance_data['dockerInspect']
        del instance_data['dockerMounts']
        docker_container = instance_data['dockerContainer']
        fields = instance_data['+fields']
        del docker_container['Created']
        del docker_container['Id']
        del docker_container['Status']
        del fields['dockerIp']
        del resp['data']['instanceHostMap']['instance']['externalId']

        assert len(docker_container['Ports']) == 1
        assert docker_container['Ports'][0]['PrivatePort'] == 8080
        assert docker_container['Ports'][0]['Type'] == 'tcp'

        instance_activate_assert_host_config(resp)
        instance_activate_assert_image_id(resp)

    event_test(agent, 'docker/instance_activate_ports', post_func=post)


@if_docker
def test_instance_activate_links_null_ports(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        links = req.data.instanceHostMap.instance.instanceLinks
        links.append(JsonObject({
            'type': 'instanceLink',
            'linkName': 'null',
            'data': {
                'fields': {
                    'ports': None
                }
            },
            'targetInstanceId': None,
        }))

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_links', pre_func=pre,
               post_func=post)


@if_docker
def test_instance_activate_double_links(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']
        inspect = docker_client().inspect_container(id)
        instance_activate_common_validation(resp)

        env = inspect['Config']['Env']

        for line in env:
            assert 'LVL' not in line

        assert 'MYSQL_NAME=/cattle/mysql' in env
        assert 'MYSQL_PORT=udp://mysql:3307' in env
        assert 'MYSQL_PORT_3307_UDP=udp://mysql:3307' in env
        assert 'MYSQL_PORT_3307_UDP_ADDR=mysql' in env
        assert 'MYSQL_PORT_3307_UDP_PORT=3307' in env
        assert 'MYSQL_PORT_3307_UDP_PROTO=udp' in env

        assert 'MYSQL_PORT_3306_TCP=tcp://mysql:3306' in env
        assert 'MYSQL_PORT_3306_TCP_ADDR=mysql' in env
        assert 'MYSQL_PORT_3306_TCP_PORT=3306' in env
        assert 'MYSQL_PORT_3306_TCP_PROTO=tcp' in env

        assert 'REDIS_NAME=/cattle/redis' in env
        assert 'REDIS_PORT=udp://redis:26' in env
        assert 'REDIS_PORT_26_UDP=udp://redis:26' in env
        assert 'REDIS_PORT_26_UDP_ADDR=redis' in env
        assert 'REDIS_PORT_26_UDP_PORT=26' in env
        assert 'REDIS_PORT_26_UDP_PROTO=udp' in env

        assert 'REDIS_ENV_ONE=TWO' in env
        assert 'REDIS_ENV_THREE=FOUR' in env
        assert 'REDIS_1_ENV_ONE=TWO' in env
        assert 'REDIS_1_ENV_THREE=FOUR' in env
        assert 'REDIS_2_ENV_ONE=TWO' in env
        assert 'REDIS_2_ENV_THREE=FOUR' in env
        assert 'ENV_REDIS_1_ENV_ONE=TWO' in env
        assert 'ENV_REDIS_1_ENV_THREE=FOUR' in env
        assert 'ENV_REDIS_2_ENV_ONE=TWO' in env
        assert 'ENV_REDIS_2_ENV_THREE=FOUR' in env

        assert 'REDIS_1_PORT=udp://redis:26' in env
        assert 'REDIS_1_PORT_26_UDP=udp://redis:26' in env
        assert 'REDIS_1_PORT_26_UDP_ADDR=redis' in env
        assert 'REDIS_1_PORT_26_UDP_PORT=26' in env
        assert 'REDIS_1_PORT_26_UDP_PROTO=udp' in env

        assert 'ENV_REDIS_1_PORT=udp://redis:26' in env
        assert 'ENV_REDIS_1_PORT_26_UDP=udp://redis:26' in env
        assert 'ENV_REDIS_1_PORT_26_UDP_ADDR=redis' in env
        assert 'ENV_REDIS_1_PORT_26_UDP_PORT=26' in env
        assert 'ENV_REDIS_1_PORT_26_UDP_PROTO=udp' in env

    event_test(agent, 'docker/instance_activate_double_links', post_func=post)


@if_docker
def test_instance_activate_links(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']
        inspect = docker_client().inspect_container(id)
        instance_activate_common_validation(resp)

        env = inspect['Config']['Env']

        assert 'MYSQL_NAME=/cattle/mysql' in env
        assert 'MYSQL_PORT=udp://mysql:3307' in env
        assert 'MYSQL_PORT_3307_UDP=udp://mysql:3307' in env
        assert 'MYSQL_PORT_3307_UDP_ADDR=mysql' in env
        assert 'MYSQL_PORT_3307_UDP_PORT=3307' in env
        assert 'MYSQL_PORT_3307_UDP_PROTO=udp' in env

        assert 'MYSQL_PORT_3306_TCP=tcp://mysql:3306' in env
        assert 'MYSQL_PORT_3306_TCP_ADDR=mysql' in env
        assert 'MYSQL_PORT_3306_TCP_PORT=3306' in env
        assert 'MYSQL_PORT_3306_TCP_PROTO=tcp' in env

        assert 'REDIS_NAME=/cattle/redis' in env
        assert 'REDIS_PORT=udp://redis:26' in env
        assert 'REDIS_PORT_26_UDP=udp://redis:26' in env
        assert 'REDIS_PORT_26_UDP_ADDR=redis' in env
        assert 'REDIS_PORT_26_UDP_PORT=26' in env
        assert 'REDIS_PORT_26_UDP_PROTO=udp' in env

        assert 'REDIS_ENV_ONE=TWO' in env
        assert 'REDIS_ENV_THREE=FOUR' in env
        assert 'REDIS_1_ENV_ONE=TWO' in env
        assert 'REDIS_1_ENV_THREE=FOUR' in env
        assert 'REDIS_2_ENV_ONE=TWO' in env
        assert 'REDIS_2_ENV_THREE=FOUR' in env
        assert 'ENV_REDIS_1_ENV_ONE=TWO' in env
        assert 'ENV_REDIS_1_ENV_THREE=FOUR' in env
        assert 'ENV_REDIS_2_ENV_ONE=TWO' in env
        assert 'ENV_REDIS_2_ENV_THREE=FOUR' in env

        assert 'REDIS_1_PORT=udp://redis:26' in env
        assert 'REDIS_1_PORT_26_UDP=udp://redis:26' in env
        assert 'REDIS_1_PORT_26_UDP_ADDR=redis' in env
        assert 'REDIS_1_PORT_26_UDP_PORT=26' in env
        assert 'REDIS_1_PORT_26_UDP_PROTO=udp' in env

        assert 'ENV_REDIS_1_PORT=udp://redis:26' in env
        assert 'ENV_REDIS_1_PORT_26_UDP=udp://redis:26' in env
        assert 'ENV_REDIS_1_PORT_26_UDP_ADDR=redis' in env
        assert 'ENV_REDIS_1_PORT_26_UDP_PORT=26' in env
        assert 'ENV_REDIS_1_PORT_26_UDP_PROTO=udp' in env

    event_test(agent, 'docker/instance_activate_links', post_func=post)


@if_docker
def test_instance_activate_links_no_service(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    delete_container('/target_redis')
    delete_container('/target_mysql')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                ports=[(3307, 'udp'), (3306, 'tcp')],
                                name='target_mysql')
    client.start(c, port_bindings={
        '3307/udp': ('127.0.0.2', 12346),
        '3306/tcp': ('127.0.0.2', 12345)
    })

    c = client.create_container('ibuildthecloud/helloworld',
                                name='target_redis')
    client.start(c)

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']
        inspect = docker_client().inspect_container(id)
        instance_activate_common_validation(resp)

        assert set(
            ['/target_mysql:/c861f990-4472-4fa1-960f-65171b544c28/mysql',
             '/target_redis:/c861f990-4472-4fa1-960f-65171b544c28/'
             'redis']) == set(inspect['HostConfig']['Links'])

    event_test(agent, 'docker/instance_activate_links_no_service',
               post_func=post)


@if_docker
def test_instance_activate_cpu_set(agent, responses):

    def pre(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuSet'] = '0,1'

    def preNull(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuSet'] = None

    def preEmpty(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuSet'] = ''

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Cpuset'] == '0,1'
        assert docker_inspect['HostConfig']['CpusetCpus'] == '0,1'
        container_field_test_boiler_plate(resp)

    def postNull(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Cpuset'] == ''
        assert docker_inspect['HostConfig']['CpusetCpus'] == ''
        container_field_test_boiler_plate(resp)

    def postEmpty(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Cpuset'] == ''
        assert docker_inspect['HostConfig']['CpusetCpus'] == ''
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)
    event_test(agent, schema, pre_func=preNull, post_func=postNull)
    event_test(agent, schema, pre_func=preEmpty, post_func=postEmpty)


@if_docker
def test_instance_activate_read_only(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    schema = 'docker/instance_activate_fields'

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['readOnly'] = True

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['ReadonlyRootfs']
        container_field_test_boiler_plate(resp)

    event_test(agent, schema, pre_func=pre, post_func=post)

    # Now test default value is False
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['HostConfig']['ReadonlyRootfs']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, post_func=post)


@if_docker
def test_instance_activate_memory_swap(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    client = docker_client()
    swap = client.info()['SwapLimit']

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['memory'] = 12000000
        instance['data']['fields']['memorySwap'] = 16000000

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        if swap:
            assert docker_inspect['Config']['MemorySwap'] == 16000000
            assert docker_inspect['HostConfig']['MemorySwap'] == 16000000
        else:
            assert docker_inspect['Config']['MemorySwap'] == -1
            assert docker_inspect['HostConfig']['MemorySwap'] == -1
        assert docker_inspect['Config']['Memory'] == 12000000
        assert docker_inspect['HostConfig']['Memory'] == 12000000
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_extra_hosts(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['extraHosts'] = ['host:1.1.1.1',
                                                    'b:2.2.2.2']

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['ExtraHosts'] == ['host:1.1.1.1',
                                                              'b:2.2.2.2']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_pid_mode(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['pidMode'] = 'host'

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['PidMode'] == 'host'
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_log_config(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['logConfig'] = \
            JsonObject({'driver': 'json-file',
                        'config': {
                            'max-size': '10',
                        }})

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['LogConfig'] == {
            'Type': 'json-file',
            'Config': {
                'max-size': '10',
            }
        }
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_log_config_null(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['logConfig'] = JsonObject({'driver': None,
                                                              'config': None})

    def pre2(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['logConfig'] = None

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['LogConfig']['Type'] == 'json-file'
        # Note: This is obscuring the fact that LogConfig.Config can be either
        # None or an empty map, but thats ok.
        assert not docker_inspect['HostConfig']['LogConfig']['Config']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    event_test(agent, schema, pre_func=pre2, post_func=post)
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    event_test(agent, schema, post_func=post)


@if_docker
def test_instance_activate_security_opt(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['securityOpt'] = ["label:foo", "label:bar"]

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['SecurityOpt'] == ["label:foo",
                                                               "label:bar"]
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_working_dir(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['workingDir'] = "/tmp"

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['WorkingDir'] == "/tmp"
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_entrypoint(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['entryPoint'] = ["./sleep.sh"]

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Entrypoint'] == ["./sleep.sh"]
        docker_container = instance_data['dockerContainer']
        docker_container['Command'] = "/sleep.sh"
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_memory(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['memory'] = 12000000

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Memory'] == 12000000
        assert docker_inspect['HostConfig']['Memory'] == 12000000
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_tty(agent, responses):

    def preFalse(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['tty'] = False

    def pre(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['tty'] = True

    def postFalse(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['Config']['Tty']
        container_field_test_boiler_plate(resp)

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Tty']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)
    event_test(agent, schema, pre_func=preFalse, post_func=postFalse)


@if_docker
def test_instance_activate_stdinOpen(agent, responses):

    def preTrueDetach(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['stdinOpen'] = True
        instance['data']['fields']['detach'] = True

    def preFalse(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['stdinOpen'] = False
        instance['data']['fields']['detach'] = False

    def pre(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['stdinOpen'] = True
        instance['data']['fields']['detach'] = False

    def postTrueDetach(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['Config']['StdinOnce']
        assert docker_inspect['Config']['OpenStdin']
        assert not docker_inspect['Config']['AttachStdin']
        container_field_test_boiler_plate(resp)

    def postFalse(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['Config']['StdinOnce']
        assert not docker_inspect['Config']['OpenStdin']
        assert not docker_inspect['Config']['AttachStdin']
        container_field_test_boiler_plate(resp)

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['StdinOnce']
        assert docker_inspect['Config']['OpenStdin']
        assert docker_inspect['Config']['AttachStdin']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)
    event_test(agent, schema, pre_func=preFalse, post_func=postFalse)
    event_test(agent, schema, pre_func=preTrueDetach, post_func=postTrueDetach)


@if_docker
def test_instance_activate_lxc_conf(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    expectedLxcConf = {"lxc.network.type": "veth"}

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['lxcConf'] = expectedLxcConf

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        for conf in docker_inspect['HostConfig']['LxcConf']:
            assert expectedLxcConf[conf['Key']] == conf['Value']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    with pytest.raises(APIError) as e:
        event_test(agent, schema, pre_func=pre, post_func=post)
    assert e.value.explanation == \
        'Cannot use --lxc-conf with execdriver: native-0.2'


@if_docker
def test_instance_activate_domainname(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['domainName'] = "rancher.io"

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Domainname'] == "rancher.io"
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_devices(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    input_devices = ['/dev/null:/dev/xnull', '/dev/random:/dev/xrandom:rw']
    expected_devices = {}
    for input_device in input_devices:
        parts_of_device = input_device.split(':')
        key = parts_of_device[0]
        expected_devices[key] = {
            "PathOnHost": parts_of_device[0],
            "PathInContainer": parts_of_device[1]
        }
        if len(parts_of_device) == 3:
            expected_devices[key]["CgroupPermissions"] = parts_of_device[2]
        else:
            expected_devices[key]["CgroupPermissions"] = "rwm"

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['devices'] = input_devices

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        actual_devices = docker_inspect['HostConfig']['Devices']

        assert len(expected_devices) == len(actual_devices)

        for act_dvc in actual_devices:
            exp_dvc = expected_devices[act_dvc['PathOnHost']]
            assert exp_dvc['PathOnHost'] == act_dvc['PathOnHost']
            assert exp_dvc['PathInContainer'] == act_dvc['PathInContainer']
            assert exp_dvc['CgroupPermissions'] == act_dvc['CgroupPermissions']

        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_dns(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['dns'] = ["1.2.3.4", "8.8.8.8"]
        instance['data']['fields']['dnsSearch'] = ["5.6.7.8", "7.7.7.7"]

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        actual_dns = docker_inspect['HostConfig']['Dns']
        actual_dns_search = docker_inspect['HostConfig']['DnsSearch']
        assert set(actual_dns) == set(["8.8.8.8", "1.2.3.4"])
        assert set(actual_dns_search) == set(["7.7.7.7", "5.6.7.8"])
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_caps(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['capAdd'] = ["MKNOD", "SYS_ADMIN"]
        instance['data']['fields']['capDrop'] = ["MKNOD", "SYS_ADMIN"]

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        set_actual_cap_add = set(docker_inspect['HostConfig']['CapAdd'])
        set_expected_cap_add = set(["MKNOD", "SYS_ADMIN"])
        assert set_actual_cap_add == set_expected_cap_add
        set_actual_cap_drop = set(docker_inspect['HostConfig']['CapDrop'])
        set_expected_cap_drop = set(["MKNOD", "SYS_ADMIN"])
        assert set_actual_cap_drop == set_expected_cap_drop
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_privileged(agent, responses):

    def preTrue(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['privileged'] = True

    def preFalse(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['privileged'] = False

    def postTrue(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['Privileged']
        container_field_test_boiler_plate(resp)

    def postFalse(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['HostConfig']['Privileged']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=preTrue, post_func=postTrue)
    event_test(agent, schema, pre_func=preFalse, post_func=postFalse)


@if_docker
def test_instance_restart_policy(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    expected_restart_pol_1 = {"maximumRetryCount": 0,
                              "name": "always"}
    expected_restart_pol_2 = {"name": "on-failure",
                              "maximumRetryCount": 2,
                              }
    expected_restart_pol_3 = {"name": "always"}

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['restartPolicy'] = expected_restart_pol_1

    def pre_failure_policy(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['restartPolicy'] = expected_restart_pol_2

    def pre_name_policy(req):
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['restartPolicy'] = expected_restart_pol_3

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        act_restart_pol = docker_inspect['HostConfig']['RestartPolicy']
        assert act_restart_pol['Name'] == expected_restart_pol_1['name']
        assert act_restart_pol['MaximumRetryCount'] == expected_restart_pol_1[
            'maximumRetryCount']
        container_field_test_boiler_plate(resp)

    def post_failure_policy(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        act_restart_pol = docker_inspect['HostConfig']['RestartPolicy']
        assert act_restart_pol['Name'] == expected_restart_pol_2['name']
        assert act_restart_pol['MaximumRetryCount'] == expected_restart_pol_2[
            'maximumRetryCount']
        container_field_test_boiler_plate(resp)

    def post_name_policy(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        act_restart_pol = docker_inspect['HostConfig']['RestartPolicy']
        assert act_restart_pol['Name'] == expected_restart_pol_3['name']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)
    event_test(agent, schema, pre_func=pre_failure_policy,
               post_func=post_failure_policy)
    event_test(agent, schema, pre_func=pre_name_policy,
               post_func=post_name_policy)


@if_docker
def test_instance_activate_cpu_shares(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuShares'] = 400

    def post(req, resp):
        instance_activate_assert_host_config(resp)
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['CpuShares'] == 400
        assert docker_inspect['HostConfig']['CpuShares'] == 400
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_ipsec(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_ipsec', post_func=post)


@if_docker
def test_instance_activate_agent_instance_localhost(agent, responses):
    CONFIG_OVERRIDE['CONFIG_URL'] = 'https://localhost:1234/a/path'
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']
        inspect = docker_client().inspect_container(id)
        instance_activate_common_validation(resp)

        port = Config.api_proxy_listen_port()
        assert 'CATTLE_CONFIG_URL_SCHEME=https' in inspect['Config']['Env']
        assert 'CATTLE_CONFIG_URL_PATH=/a/path' in inspect['Config']['Env']
        assert 'CATTLE_CONFIG_URL_PORT={0}'.format(port) in \
            inspect['Config']['Env']

    event_test(agent, 'docker/instance_activate_agent_instance',
               post_func=post)


@if_docker
def test_instance_activate_agent_instance(agent, responses):
    CONFIG_OVERRIDE['CONFIG_URL'] = 'https://something.fake:1234/a/path'
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']

        inspect = docker_client().inspect_container(id)
        instance_activate_common_validation(resp)

        port = Config.api_proxy_listen_port()
        assert 'CATTLE_CONFIG_URL={0}'.format(Config.config_url()) in \
               inspect['Config']['Env']
        assert 'CATTLE_CONFIG_URL_SCHEME=https' not in inspect['Config']['Env']
        assert 'CATTLE_CONFIG_URL_PATH=/a/path' not in inspect['Config']['Env']
        assert 'CATTLE_CONFIG_URL_PORT={0}'.format(port) not in \
               inspect['Config']['Env']
        assert 'ENV1=value1' in inspect['Config']['Env']

    event_test(agent, 'docker/instance_activate_agent_instance',
               post_func=post)


@if_docker
def test_instance_activate_volumes(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    delete_container('/target_volumes_from_by_uuid')
    delete_container('/target_volumes_from_by_id')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                volumes=['/volumes_from_path_by_uuid'],
                                name='target_volumes_from_by_uuid')
    client.start(c)

    c2 = client.create_container('ibuildthecloud/helloworld',
                                 volumes=['/volumes_from_path_by_id'],
                                 name='target_volumes_from_by_id')
    client.start(c2)

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['dataVolumesFromContainers'][1]['externalId'] = c2['Id']

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        inspect = instance_data['dockerInspect']

        assert inspect['Volumes']['/host/proc'] == '/proc'
        assert inspect['Volumes']['/host/sys'] == '/sys'
        assert inspect['Volumes']['/random'] is not None
        assert inspect['Volumes']['/volumes_from_path_by_uuid'] is not None
        assert inspect['Volumes']['/volumes_from_path_by_id'] is not None

        assert len(inspect['Volumes']) == 5

        assert inspect['VolumesRW'] == {
            '/host/proc': True,
            '/host/sys': False,
            '/random': True,
            '/volumes_from_path_by_uuid': True,
            '/volumes_from_path_by_id': True,

        }

        assert set(['/sys:/host/sys:ro', '/proc:/host/proc:rw']) == set(
            inspect['HostConfig']['Binds'])

        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_volumes', pre_func=pre,
               post_func=post)


@if_docker
def test_instance_activate_null_command(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_command_null', post_func=post)


@if_docker
def test_instance_activate_command(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_command', post_func=post)


@if_docker
def test_instance_activate_command_args(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_command_args', post_func=post)


@if_docker
def test_instance_deactivate(agent, responses):
    instance_only_activate(agent, responses)

    def post(req, resp):
        container_field_test_boiler_plate(resp)

    start = time.time()
    event_test(agent, 'docker/instance_deactivate', post_func=post)
    end = time.time()

    assert end - start < 1.2

    def pre(req):
        req['data']['processData']['timeout'] = 1

    instance_only_activate(agent, responses)
    start = time.time()
    event_test(agent, 'docker/instance_deactivate', pre_func=pre,
               post_func=post)
    end = time.time()

    assert end - start > 1


def assert_ping_stat_resources(resp):
    hostname = Config.hostname()
    pool_name = hostname + ' Storage Pool'
    assert resp['data']['resources'][0]['name'] == hostname
    assert resp['data']['resources'][1]['name'] == pool_name
    resp['data']['resources'][0]['name'] = 'localhost'
    resp['data']['resources'][1]['name'] = 'localhost Storage Pool'


def ping_post_process(req, resp):
    resources = resp['data']['resources']

    labels = {'io.rancher.host.docker_version': '1.6',
              'io.rancher.host.linux_kernel_version': '4.1'}

    uuids = {'uuid-running': 0, 'uuid-stopped': 1, 'uuid-created': 2,
             'uuid-system': 3, 'uuid-sys-nover': 4, 'uuid-agent-instance': 5}
    instances = []
    for r in resources:
        if r['type'] == 'host':
            if 'io.rancher.host.kvm' in r['labels']:
                assert r['labels']['io.rancher.host.kvm'] == 'true'
                del r['labels']['io.rancher.host.kvm']
            assert len(r['labels']) == 2
            r['labels'] = labels
        if r['type'] == 'instance' and r['uuid'] in uuids:
            if r['uuid'] == 'uuid-running':
                assert r['state'] == 'running'
            elif r['uuid'] in ['uuid-stopped', 'uuid-agent-instance']:
                assert r['state'] == 'stopped'
            elif r['uuid'] == 'uuid-system' or r['uuid'] == 'uuid-sys-nover':
                assert r['state'] == 'stopped'
                # Account for docker 1.7/1.8 difference
                try:
                    del r['labels']['io.rancher.container.system']
                except KeyError:
                    pass

            # Account for docker 1.6 where ':latest' is appended
            if r['uuid'] == 'uuid-sys-nover' and r[
                    'image'] == 'rancher/agent:latest':
                r['image'] = 'rancher/agent'

            assert r['dockerId'] is not None
            del r['dockerId']
            assert r['created'] is not None
            del r['created']
            instances.append(r)

    def ping_sort(item):
        return uuids[item['uuid']]

    instances.sort(key=ping_sort)

    assert len(instances) == 5

    resources = filter(lambda x: x.get('kind') == 'docker', resources)
    resources += instances
    resp['data']['resources'] = resources
    assert_ping_stat_resources(resp)


def ping_post_process_state_exception(req, resp):
    labels = {'io.rancher.host.docker_version': '1.6',
              'io.rancher.host.linux_kernel_version': '4.1'}

    # This filters down the returned resources to just the stat-based ones.
    # In other words, it gets rid of all containers from the response.
    resp['data']['resources'] = filter(lambda x: x.get('kind') == 'docker',
                                       resp['data']['resources'])
    for r in resp['data']['resources']:
        if r['type'] == 'host':
            if 'io.rancher.host.kvm' in r['labels']:
                assert r['labels']['io.rancher.host.kvm'] == 'true'
                del r['labels']['io.rancher.host.kvm']
            assert len(r['labels']) == 2
            r['labels'] = labels

    assert_ping_stat_resources(resp)


@if_docker
def test_ping(pull_images, agent, responses, mocker):
    mocker.patch.object(HostInfo, 'collect_data',
                        return_value=json_data('docker/host_info_resp'))

    client = docker_client()

    delete_container('/named-running')
    delete_container('/named-stopped')
    delete_container('/named-created')
    delete_container('/named-system')
    delete_container('/named-sys-nover')
    delete_container('/named-agent-instance')

    client.create_container('ibuildthecloud/helloworld',
                            name='named-created', labels={
                                'io.rancher.container.uuid': 'uuid-created'})
    running = client.create_container('ibuildthecloud/helloworld:latest',
                                      name='named-running', labels={
                                          'io.rancher.container.uuid':
                                          'uuid-running'})
    client.start(running)
    stopped = client.create_container('ibuildthecloud/helloworld:latest',
                                      name='named-stopped', labels={
                                          'io.rancher.container.uuid':
                                          'uuid-stopped'})
    client.start(stopped)
    client.kill(stopped, signal='SIGKILL')

    system_con = client.create_container('rancher/agent:v0.7.9',
                                         name='named-system', labels={
                                             'io.rancher.container.uuid':
                                             'uuid-system'})
    client.start(system_con)
    client.kill(system_con, signal='SIGKILL')

    sys_nover = client.create_container('rancher/agent',
                                        name='named-sys-nover', labels={
                                            'io.rancher.container.uuid':
                                            'uuid-sys-nover'})
    client.start(sys_nover)
    client.kill(sys_nover, signal='SIGKILL')

    agent_inst_con = client.create_container(
        'ibuildthecloud/helloworld:latest',
        name='named-agent-instance',
        labels={
            'io.rancher.container.uuid':
                'uuid-agent-instance',
            'io.rancher.container.system':
                'networkAgent'},
        command='true')
    client.start(agent_inst_con)
    client.kill(agent_inst_con, signal='SIGKILL')

    CONFIG_OVERRIDE['DOCKER_UUID'] = 'testuuid'
    CONFIG_OVERRIDE['PHYSICAL_HOST_UUID'] = 'hostuuid'

    event_test(agent, 'docker/ping', post_func=ping_post_process)


@if_docker
def test_ping_stat_exception(agent, responses, mocker):
    mocker.patch.object(HostInfo, 'collect_data',
                        side_effect=ValueError('Bad Value Found'))

    CONFIG_OVERRIDE['DOCKER_UUID'] = 'testuuid'
    CONFIG_OVERRIDE['PHYSICAL_HOST_UUID'] = 'hostuuid'

    event_test(agent, 'docker/ping_stat_exception',
               post_func=ping_post_process_state_exception)


@if_docker
def test_volume_purge(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    delete_container('/target_volumes_from')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                volumes=['/volumes_from_path'],
                                name='target_volumes_from')
    client.start(c)
    # TODO Figure out a better way to test this. Because purging a volume
    # means removing it from disk, we run into trouble testing when
    # boot2docker is in the picture because the locally running agent cannot
    # see inside the b2d vm. We do currently test this functionality fully
    # in the integration test suite.
    event_test(agent, 'docker/volume_purge')


@if_docker
def test_instance_activate_ipsec_network_agent(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_ipsec_network_agent',
               post_func=post)


@if_docker
def test_instance_activate_ipsec_lb_agent(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_ipsec_lb_agent',
               post_func=post)


@if_docker
def test_instance_force_stop(agent, responses):
    delete_container('/force-stop-test')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                name='force-stop-test')
    client.start(c)
    inspect = client.inspect_container(c)
    assert inspect['State']['Running'] is True

    def pre(req):
        req['data']['instanceForceStop']['id'] = c['Id']

    def post(req, resp):
        inspect = client.inspect_container(c)
        assert inspect['State']['Running'] is False

    event_test(agent, 'docker/instance_force_stop',
               pre_func=pre, post_func=post, diff=False)

    # Assert that you can call on a stop container without issue
    event_test(agent, 'docker/instance_force_stop',
               pre_func=pre, post_func=post, diff=False)

    # And a non-existent one
    client.remove_container(c)
    event_test(agent, 'docker/instance_force_stop', pre_func=pre, diff=False)


@if_docker
def test_instance_remove(agent, responses):
    instance_only_activate(agent, responses)
    container = get_container('/c861f990-4472-4fa1-960f-65171b544c28')
    assert container is not None

    def post(req, resp):
        c = get_container('/c861f990-4472-4fa1-960f-65171b544c28')
        assert c is None
    event_test(agent, 'docker/instance_remove', post_func=post)

    # Test finding and removing by externalId instead of uuid
    instance_only_activate(agent, responses)
    container = get_container('/c861f990-4472-4fa1-960f-65171b544c28')
    assert container is not None

    def pre(req):
        req['data']['instanceHostMap']['instance']['externalId'] = container[
            'Id']
        req['data']['instanceHostMap']['instance']['uuid'] = 'wont be found'

    def post(req, resp):
        c = get_container('/c861f990-4472-4fa1-960f-65171b544c28')
        assert c is None
    event_test(agent, 'docker/instance_remove', pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_labels(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_labels',
               post_func=post)


@if_docker
def test_404_on_remove():
    # This tests the functionality in DockerCompute._do_instance_remove(),
    # but recreating a scenario where a 404 is returned on a delete is pretty
    # difficult from the event_test framework, so just testing the function
    # ecplicitly
    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                name='double_remove')

    client.remove_container(c, force=True, v=True)
    remove_container(client, c)
    remove_container(client, c)


@if_docker
def test_no_label_field():
    delete_container('/no-label-test')
    client = docker_client()
    dc = DockerCompute()

    c = client.create_container('ibuildthecloud/helloworld',
                                name='no-label-test')
    instance = JsonObject({
        'uuid': 'irrelevant',
        'externalId': c['Id']
    })
    container = dc.get_container(client, instance)
    containers = []

    # No Labels scenario
    del container['Labels']
    dc.add_container('running', container, containers)
    assert containers[0]['uuid'] == 'no-label-test'
    assert containers[0]['systemContainer'] is None

    # None value for Labels scenario
    containers = []
    container['Labels'] = None
    dc.add_container('running', container, containers)
    assert containers[0]['uuid'] == 'no-label-test'
    assert containers[0]['systemContainer'] is None


@if_docker
def test_instance_links_net_host(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    delete_container('/target_redis')
    delete_container('/target_mysql')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                ports=[(3307, 'udp'), (3306, 'tcp')],
                                name='target_mysql')
    client.start(c, port_bindings={
        '3307/udp': ('127.0.0.2', 12346),
        '3306/tcp': ('127.0.0.2', 12345)
    })

    c = client.create_container('ibuildthecloud/helloworld',
                                name='target_redis')
    client.start(c)

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['nics'][0]['network']['kind'] = 'dockerHost'

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']
        inspect = docker_client().inspect_container(id)
        assert inspect['HostConfig']['Links'] is None

    event_test(agent, 'docker/instance_activate_links_no_service',
                      pre_func=pre, post_func=post, diff=False)


@if_docker
def test_volume_from_data_volume_mounts_with_opt(agent, responses, request):
    driver_opts = JsonObject(
        {'foo': 'bar'}
    )
    volumes_from_data_volume_mounts_test(agent, responses, request,
                                         driver_opts=driver_opts)


@if_docker
def test_volume_from_data_volume_mounts(agent, responses, request):
    volumes_from_data_volume_mounts_test(agent, responses, request)


@if_docker
def test_volume_from_data_volume_mounts_empty_opts(agent, responses, request):
    volumes_from_data_volume_mounts_test(agent, responses, request,
                                         driver_opts=JsonObject({}))


def volumes_from_data_volume_mounts_test(agent, responses, request,
                                         driver_opts=None):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    delete_container('/convoy')
    client = docker_client(version='1.21')
    dr = 'convoy%s' % random_str()
    _launch_convoy_container(client, dr)

    vol_name = 'test-vol1'

    # Doing redundant cleanup as a finalizer because things can get weird if
    # volume drivers just disappear while volumes for it are still around
    def remove_vol():
        delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        client.remove_volume(vol_name)
        delete_container('/convoy')
    request.addfinalizer(remove_vol)

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['dataVolumes'] = ['%s:/con/path' % vol_name]
        mounts = [JsonObject(
            {
                'name': vol_name,
                'data': {
                    'fields': {
                        'driver': dr,
                        'driverOpts': driver_opts,
                    },
                },
            })]
        instance['volumesFromDataVolumeMounts'] = mounts

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        mounts = instance_data['dockerMounts']
        assert len(mounts) == 1
        assert mounts[0]['Name'] == vol_name
        assert mounts[0]['Driver'] == dr

    event_test(agent, 'docker/instance_activate', pre_func=pre, post_func=post,
               diff=False)


def _launch_convoy_container(client, dr):
    client.pull('cjellick/convoy-local', 'v0.1.0')
    container = client. \
        create_container('cjellick/convoy-local:v0.1.0',
                         name='/convoy',
                         environment={
                             'CONVOY_SOCKET': '/var/run/%s.sock' % dr,
                             'CONVOY_DATA_DIR': '/tmp/%s' % dr,
                             'CONVOY_DRIVER_NAME': '%s' % dr},
                         volumes=['/var/run/',
                                  '/etc/docker/plugins',
                                  '/tmp/%s' % dr],
                         host_config=client.
                         create_host_config(privileged=True, binds=[
                             '/var/run:/var/run',
                             '/etc/docker/plugins/:/etc/docker/plugins',
                             '/tmp/%s:/tmp/%s' % (dr, dr)])
                         )
    client.start(container)
    return container
