from docker.errors import APIError
from cattle.plugins.docker import docker_client

# TODO cattle.plugins.load_plugins() somehow make cattle.plugin.* modules
# unavailable, importing it first
import cattle.plugins.docker  # NOQA

from cattle.plugins.docker.network import setup_mac_and_ip

from cattle.plugins.host_info.main import HostInfo
from .common_fixtures import *  # NOQA
import pytest
import time
from cattle import CONFIG_OVERRIDE, Config

if_docker = pytest.mark.skipif('os.environ.get("DOCKER_TEST") == "false"',
                               reason='DOCKER_TEST is not set')


CONFIG_OVERRIDE['DOCKER_HOST_IP'] = '1.2.3.4'


def _delete_container(name):
    client = docker_client()
    for c in client.containers(all=True):
        for container_name in c['Names']:
            if name == container_name:
                try:
                    client.kill(c)
                except:
                    pass
                client.remove_container(c)


def _get_container(name):
    client = docker_client()
    for c in client.containers(all=True):
        for container_name in c['Names']:
            if name == container_name:
                return c
    return None


@if_docker
def test_image_list():
    c = docker_client()
    images = c.images(all=True)
    if len(images) == 0:
        c.pull('busybox')

    images = c.images(all=True)

    assert 'Id' in images[0]
    assert 'ID' not in images[0]


@if_docker
def test_image_activate(agent, responses):
    try:
        docker_client().remove_image('ibuildthecloud/helloworld:latest')
    except APIError:
        pass

    def post(req, resp):
        image_data = resp['data']['imageStoragePoolMap']['+data']
        del image_data['dockerImage']['VirtualSize']

    event_test(agent, 'docker/image_activate', post_func=post)


@if_docker
def test_volume_activate(agent, responses):
    event_test(agent, 'docker/volume_activate')


@if_docker
def test_volume_deactivate(agent, responses):
    event_test(agent, 'docker/volume_deactivate')


@if_docker
def test_instance_activate_need_pull_image(agent, responses):
    try:
        docker_client().remove_image('ibuildthecloud/helloworld:latest')
    except APIError:
        pass

    test_instance_only_activate(agent, responses)


@if_docker
def test_image_activate_no_reg_cred_pull_image(agent, responses):
    try:
        docker_client().remove_image('ibuildthecloud/helloworld:latest')
    except APIError:
        pass

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        image['registryCredential'] = None

    def post(req, resp):
        image_data = resp['data']['imageStoragePoolMap']['+data']
        del image_data['dockerImage']['VirtualSize']

    event_test(agent, 'docker/image_activate', pre_func=pre, post_func=post)


def _pull_image_by_name(agent, responses, image_name):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    try:
        docker_client().remove_image(image_name)
    except APIError:
        pass

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        remap_dockerImage(image, image_name)

    def post(req, resp):
        responseImage = resp['data']['imageStoragePoolMap']['+data']
        responseImage = responseImage['dockerImage']
        correct = False
        sent_parsed = _parse_repo_tag(image_name)
        for resp_img_uuid in responseImage['RepoTags']:
            parsed_name = _parse_repo_tag(resp_img_uuid)
            assert parsed_name['repository'] == sent_parsed['repository']
            if sent_parsed['tag'] != '':
                if sent_parsed['tag'] == 'latest':
                    if parsed_name['tag'] is not None:
                        correct = True
                else:
                    if parsed_name['tag'] == sent_parsed['tag']:
                        correct = True
            else:
                correct = True
        assert correct is True

    event_test(agent, 'docker/image_activate', pre_func=pre, post_func=post,
               no_diff=True)


def remap_dockerImage(dockerImage, image_name):
    image = dockerImage
    parsed = _parse_repo_tag(image_name)
    image['name'] = parsed['fullName']
    image['uuid'] = 'docker:' + parsed['fullName']
    image['data']['dockerImage']['fullName'] = parsed['fullName']
    image['data']['dockerImage']['server'] = parsed['server']
    image['data']['dockerImage']['repository'] = parsed['repository']
    image['data']['dockerImage']['lookUpName'] = parsed['lookUpName']
    image['data']['dockerImage']['qualifiedName'] = parsed['qualifiedName']
    image['data']['dockerImage']['namespace'] = parsed['namespace']
    image['data']['dockerImage']['tag'] = parsed['tag']


@if_docker
def test_image_pull_variants(agent, responses):
    image_names = [
        'ibuildthecloud/helloworld:latest',
        'ibuildthecloud/helloworld',
        'tianon/true',
        'tianon/true:latest',
        # 'registry.rancher.io/rancher/scratch', Need to make our registry
        # 'registry.rancher.io/rancher/scratch:latest', Support non-authed
        # 'registry.rancher.io/rancher/scratch:new_stuff',  pulls.
        'cirros',
        'cirros:latest',
        'cirros:0.3.3'
    ]

    for i in image_names:
        _pull_image_by_name(agent, responses, i)


@if_docker
def _test_image_pull_credential(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    image_name = 'registry.rancher.io/rancher/loop'

    try:
        docker_client().remove_image(image_name)
    except APIError:
        pass

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        remap_dockerImage(image, image_name)
        image['registryCredential'] = {
            'publicValue': 'rancher',
            'secretValue': 'rancher',
            'data': {
                'fields': {
                    'email': 'test@rancher.com',
                }
            },
            'registry': {
                'data': {
                    'fields': {
                        'serverAddress': 'registry.rancher.io'
                    }
                }
            }
        }

    def post(req, resp):
        responseImage = resp['data']['imageStoragePoolMap']['+data']
        responseImage = responseImage['dockerImage']
        correct = False
        sent_parsed = _parse_repo_tag(image_name)
        for resp_img_uuid in responseImage['RepoTags']:
            parsed_name = _parse_repo_tag(resp_img_uuid)
            assert parsed_name['repository'] == sent_parsed['repository']
            if sent_parsed['tag'] != '':
                if sent_parsed['tag'] == 'latest':
                    if parsed_name['tag'] is not None:
                        correct = True
                else:
                    if parsed_name['tag'] == sent_parsed['tag']:
                        correct = True
            else:
                correct = True
        assert correct is True

    event_test(agent, 'docker/image_activate', pre_func=pre, post_func=post,
               no_diff=True)


@if_docker
def _test_instance_pull_credential(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    image_name = 'registry.rancher.io/rancher/loop'

    try:
        docker_client().remove_image(image_name)
    except APIError:
        pass

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        image = instance['image']
        remap_dockerImage(image, image_name)
        image['registryCredential'] = {
            'publicValue': 'rancher',
            'secretValue': 'rancher',
            'data': {
                'fields': {
                    'email': 'test@rancher.com',
                }
            },
            'registry': {
                'data': {
                    'fields': {
                        'serverAddress': 'registry.rancher.io'
                    }
                }
            }
        }

    def post(req, resp):
        responseInstance = resp['data']['instanceHostMap']['instance']['+data']
        resp_img_uuid = responseInstance['dockerContainer']['Image']
        parsed_name = _parse_repo_tag(resp_img_uuid)
        sent_parsed = _parse_repo_tag(image_name)
        assert parsed_name['repository'] == sent_parsed['repository']
        if sent_parsed['tag'] != '':
            if sent_parsed['tag'] == 'latest':
                assert parsed_name['tag'] is not None
            else:
                assert parsed_name['tag'] == sent_parsed['tag']

    event_test(agent, 'docker/instance_activate', pre_func=pre, post_func=post,
               no_diff=True)


def image_pull_invalid_credential(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    image_name = 'quay.io/wizardofmath/whisperdocker'

    try:
        docker_client().remove_image(image_name)
    except APIError:
        pass

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        for nic in instance['nics']:
            nic['macAddress'] = ''
        instance['data']['fields']['imageUuid'] = image_name
        instance['registryCredential'] = {
            'publicValue': 'wizardofmath+whisper',
            'secretValue': 'W0IUYDBM2VORHM4DTTEHSMKLXGCG3KD',
            'data': {
                'fields': {
                    'email': 'wizardofmath+whisper@gmail.com',
                }
            },
            'storagePool': {
                'data': {
                    'fields': {
                        'serveraddress': 'https://quay.io/v1/'
                    }
                }
            }
            }

    def post(req, resp):
        responseInstance = resp['data']['instanceHostMap']['instance']['+data']
        resp_img_uuid = responseInstance['dockerContainer']['Image']
        parsed_name = _parse_repo_tag(resp_img_uuid)
        sent_parsed = _parse_repo_tag(image_name)
        assert parsed_name['repo'] == sent_parsed['repo']
        if sent_parsed['tag'] != '':
            if sent_parsed['tag'] == 'latest':
                assert parsed_name['tag'] is not None
            else:
                assert parsed_name['tag'] == sent_parsed['tag']
        responseInstance['dockerContainer']['Image'] =\
            'ibuildthecloud/helloworld:latest'
        responseInstance['dockerContainer']['Command'] = '/sleep.sh'

        instance_activate_common_validation(resp)

    event_test(agent, 'docker/image_activate', pre_func=pre, post_func=post)


@if_docker
def test_instance_only_activate(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        for nic in instance['nics']:
            nic['macAddress'] = ''

    def post(req, resp):
        docker_inspect = resp['data']['instanceHostMap']['instance']['+data'][
            'dockerInspect']
        envs = docker_inspect['Config']['Env']
        found_ip_env = False
        ip = req['data']['instanceHostMap']['instance']['nics'][
            0]['ipAddresses'][0]
        expected_ip_env_var = "RANCHER_IP={0}/{1}".format(ip.address,
                                                          ip.subnet.cidrSize)
        for env in envs:
            if env == expected_ip_env_var:
                found_ip_env = True
                break
        assert found_ip_env
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate', pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_no_mac_address(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        del instance_data['dockerInspect']
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

    event_test(agent, 'docker/instance_activate_ports', post_func=post)


@if_docker
def test_instance_activate_links(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        id = resp['data']['instanceHostMap']['instance']
        id = id['+data']['dockerContainer']['Id']
        inspect = docker_client().inspect_container(id)
        instance_activate_common_validation(resp)

        env = inspect['Config']['Env']

        assert 'MYSQL_NAME=/cattle/mysql' in env
        assert 'MYSQL_PORT=udp://127.0.0.2:12346' in env
        assert 'MYSQL_PORT_3307_UDP=udp://127.0.0.2:12346' in env
        assert 'MYSQL_PORT_3307_UDP_ADDR=127.0.0.2' in env
        assert 'MYSQL_PORT_3307_UDP_PORT=12346' in env
        assert 'MYSQL_PORT_3307_UDP_PROTO=udp' in env

        assert 'MYSQL_PORT_3306_TCP=tcp://127.0.0.1:12345' in env
        assert 'MYSQL_PORT_3306_TCP_ADDR=127.0.0.1' in env
        assert 'MYSQL_PORT_3306_TCP_PORT=12345' in env
        assert 'MYSQL_PORT_3306_TCP_PROTO=tcp' in env

        assert 'REDIS_NAME=/cattle/redis' in env
        assert 'REDIS_PORT=udp://127.0.0.1:23456' in env
        assert 'REDIS_PORT_26_UDP=udp://127.0.0.1:23456' in env
        assert 'REDIS_PORT_26_UDP_ADDR=127.0.0.1' in env
        assert 'REDIS_PORT_26_UDP_PORT=23456' in env
        assert 'REDIS_PORT_26_UDP_PROTO=udp' in env

    event_test(agent, 'docker/instance_activate_links', post_func=post)


@if_docker
def test_instance_activate_links_no_service(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    _delete_container('/target_redis')
    _delete_container('/target_mysql')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                ports=['3307/udp', '3306/tcp'],
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
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuSet'] = '0,1'

    def preNull(req):
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuSet'] = None

    def preEmpty(req):
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuSet'] = ''

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Cpuset'] == '0,1'
        container_field_test_boiler_plate(resp)

    def postNull(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Cpuset'] == ''
        container_field_test_boiler_plate(resp)

    def postEmpty(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Cpuset'] == ''
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)
    event_test(agent, schema, pre_func=preNull, post_func=postNull)
    event_test(agent, schema, pre_func=preEmpty, post_func=postEmpty)


@if_docker
def test_instance_activate_memory_swap(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['memory'] = 8000000
        instance['data']['fields']['memorySwap'] = 16000000

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['MemorySwap'] == 16000000
        assert docker_inspect['Config']['Memory'] == 8000000
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_entrypoint(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['entryPoint'] = ["./sleep.sh"]

    def post(req, resp):
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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['memory'] = 8000000

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Memory'] == 8000000
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_tty(agent, responses):

    def preFalse(req):
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['tty'] = False

    def pre(req):
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['tty'] = True

    def postFalse(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['Config']['Tty']
        container_field_test_boiler_plate(resp)

    def post(req, resp):
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
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['stdinOpen'] = True
        instance['data']['fields']['detach'] = True

    def preFalse(req):
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['stdinOpen'] = False
        instance['data']['fields']['detach'] = False

    def pre(req):
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['stdinOpen'] = True
        instance['data']['fields']['detach'] = False

    def postTrueDetach(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['Config']['StdinOnce']
        assert docker_inspect['Config']['OpenStdin']
        assert not docker_inspect['Config']['AttachStdin']
        container_field_test_boiler_plate(resp)

    def postFalse(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['Config']['StdinOnce']
        assert not docker_inspect['Config']['OpenStdin']
        assert not docker_inspect['Config']['AttachStdin']
        container_field_test_boiler_plate(resp)

    def post(req, resp):
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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
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
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_domainname(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['domainName'] = "rancher.io"

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['Domainname'] == "rancher.io"
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_devices(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['dns'] = ["1.2.3.4", "8.8.8.8"]
        instance['data']['fields']['dnsSearch'] = ["5.6.7.8", "7.7.7.7"]

    def post(req, resp):
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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['capAdd'] = ["MKNOD", "SYS_ADMIN"]
        instance['data']['fields']['capDrop'] = ["MKNOD", "SYS_ADMIN"]

    def post(req, resp):
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
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['privileged'] = True

    def preFalse(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['privileged'] = False

    def postTrue(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['HostConfig']['Privileged']
        container_field_test_boiler_plate(resp)

    def postFalse(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['HostConfig']['Privileged']
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=preTrue, post_func=postTrue)
    event_test(agent, schema, pre_func=preFalse, post_func=postFalse)


@if_docker
def test_instance_restart_policy(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
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
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['restartPolicy'] = expected_restart_pol_2

    def pre_name_policy(req):
        _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['restartPolicy'] = expected_restart_pol_3

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        act_restart_pol = docker_inspect['HostConfig']['RestartPolicy']
        assert act_restart_pol['Name'] == expected_restart_pol_1['name']
        assert act_restart_pol['MaximumRetryCount'] == expected_restart_pol_1[
            'maximumRetryCount']
        container_field_test_boiler_plate(resp)

    def post_failure_policy(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        act_restart_pol = docker_inspect['HostConfig']['RestartPolicy']
        assert act_restart_pol['Name'] == expected_restart_pol_2['name']
        assert act_restart_pol['MaximumRetryCount'] == expected_restart_pol_2[
            'maximumRetryCount']
        container_field_test_boiler_plate(resp)

    def post_name_policy(req, resp):
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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['data']['fields']['cpuShares'] = 400

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['Config']['CpuShares'] == 400
        container_field_test_boiler_plate(resp)

    schema = 'docker/instance_activate_fields'
    event_test(agent, schema, pre_func=pre, post_func=post)


@if_docker
def test_instance_activate_ipsec(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_ipsec', post_func=post)


@if_docker
def test_instance_activate_agent_instance_localhost(agent, responses):
    CONFIG_OVERRIDE['CONFIG_URL'] = 'https://localhost:1234/a/path'
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

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
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

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


def _sort_ports(docker_container):
    docker_container['Ports'] = sorted(docker_container['Ports'],
                                       key=lambda x: 1-x['PrivatePort'])
    return docker_container


@if_docker
def test_instance_activate_volumes(agent, responses):
    _delete_container('/c-c861f990-4472-4fa1-960f-65171b544c28')

    _delete_container('/target_volumes_from')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                volumes=['/volumes_from_path'],
                                name='target_volumes_from')
    client.start(c)

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        inspect = instance_data['dockerInspect']

        assert inspect['Volumes']['/host/proc'] == '/proc'
        assert inspect['Volumes']['/host/sys'] == '/sys'
        assert inspect['Volumes']['/random'] is not None
        assert inspect['Volumes']['/volumes_from_path'] is not None

        assert len(inspect['Volumes']) == 4

        assert inspect['VolumesRW'] == {
            '/host/proc': True,
            '/host/sys': False,
            '/random': True,
            '/volumes_from_path': True,

        }

        assert set(['/sys:/host/sys:ro', '/proc:/host/proc:rw']) == set(
            inspect['HostConfig']['Binds'])

        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_volumes', post_func=post)


@if_docker
def test_instance_activate_null_command(agent, responses):
    _delete_container('/c-c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_command_null', post_func=post)


@if_docker
def test_instance_activate_command(agent, responses):
    _delete_container('/c-c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_command', post_func=post)


@if_docker
def test_instance_activate_command_args(agent, responses):
    _delete_container('/ca-c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_command_args', post_func=post)


@if_docker
def test_instance_deactivate(agent, responses):
    test_instance_only_activate(agent, responses)

    def post(req, resp):
        container_field_test_boiler_plate(resp)

    start = time.time()
    event_test(agent, 'docker/instance_deactivate', post_func=post)
    end = time.time()

    assert end - start < 1.2

    def pre(req):
        req['data']['processData']['timeout'] = 1

    test_instance_only_activate(agent, responses)
    start = time.time()
    event_test(agent, 'docker/instance_deactivate', pre_func=pre,
               post_func=post)
    end = time.time()

    assert end - start > 1


def ping_post_process(req, resp):
        hostname = Config.hostname()
        pool_name = hostname + ' Storage Pool'
        resources = resp['data']['resources']

        uuid = 'c861f990-4472-4fa1-960f-65171b544c28'
        instances = filter(lambda x: x['type'] == 'instance' and
                           x['uuid'] == uuid, resources)
        assert len(instances) == 1
        instance = instances[0]
        assert instance['dockerId'] is not None
        del instance['dockerId']

        resources = filter(lambda x: x.get('kind') == 'docker', resources)
        resources.append(instance)

        resp['data']['resources'] = resources

        assert resp['data']['resources'][0]['name'] == hostname
        assert resp['data']['resources'][1]['name'] == pool_name
        resp['data']['resources'][0]['name'] = 'localhost'
        resp['data']['resources'][1]['name'] = 'localhost Storage Pool'


@if_docker
def test_ping(agent, responses, mocker):
    mocker.patch.object(HostInfo, 'collect_data',
                        return_value=json_data('docker/host_info_resp'))

    test_instance_only_activate(agent, responses)

    CONFIG_OVERRIDE['DOCKER_UUID'] = 'testuuid'
    CONFIG_OVERRIDE['PHYSICAL_HOST_UUID'] = 'hostuuid'

    event_test(agent, 'docker/ping', post_func=ping_post_process)


@if_docker
def test_ping_stat_exception(agent, responses, mocker):
    mocker.patch.object(HostInfo, 'collect_data',
                        side_effect=ValueError('Bad Value Found'))

    test_instance_only_activate(agent, responses)

    CONFIG_OVERRIDE['DOCKER_UUID'] = 'testuuid'
    CONFIG_OVERRIDE['PHYSICAL_HOST_UUID'] = 'hostuuid'

    event_test(agent, 'docker/ping_stat_exception',
               post_func=ping_post_process)


@if_docker
def test_volume_purge(agent, responses):
    _delete_container('/c-c861f990-4472-4fa1-960f-65171b544c28')
    _delete_container('/target_volumes_from')

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


def container_field_test_boiler_plate(resp):
    instance_data = resp['data']['instanceHostMap']['instance']['+data']
    docker_container = instance_data['dockerContainer']
    assert resp['data']['instanceHostMap']['instance']['externalId'] == \
        instance_data['dockerInspect']['Id']
    del resp['data']['instanceHostMap']['instance']['externalId']
    del instance_data['dockerInspect']
    fields = instance_data['+fields']
    del docker_container['Created']
    del docker_container['Id']
    del docker_container['Status']
    del fields['dockerIp']
    _sort_ports(docker_container)


def instance_activate_common_validation(resp):
    container_field_test_boiler_plate(resp)
    docker_container = resp['data']['instanceHostMap']['instance']
    docker_container = docker_container['+data']['dockerContainer']
    fields = resp['data']['instanceHostMap']['instance']['+data']['+fields']
    del docker_container['Ports'][0]['PublicPort']
    del docker_container['Ports'][1]['PublicPort']
    assert fields['dockerPorts']['8080/tcp'] is not None
    assert fields['dockerPorts']['12201/udp'] is not None
    fields['dockerPorts']['8080/tcp'] = '1234'
    fields['dockerPorts']['12201/udp'] = '5678'


@if_docker
def test_instance_activate_ipsec_network_agent(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_ipsec_network_agent',
               post_func=post)


@if_docker
def test_instance_activate_ipsec_lb_agent(agent, responses):
    _delete_container('/c861f990-4472-4fa1-960f-65171b544c28')

    def post(req, resp):
        instance_activate_common_validation(resp)

    event_test(agent, 'docker/instance_activate_ipsec_lb_agent',
               post_func=post)


def _parse_repo_tag(image):
        namespace = None
        repo = None
        tag = None
        server = 'index.docker.io'
        if image is None:
            return None
        forwardSlash = image.split("/")
        if len(forwardSlash) <= 3:
            if len(forwardSlash) == 1:
                split2 = forwardSlash[0].split(":")
                if len(split2) == 1:
                    tag = "latest"
                    repo = image
                elif len(split2) == 2:
                    tag = split2[1]
                    repo = split2[0]
            elif len(forwardSlash) == 2:
                first = forwardSlash[0]
                second = forwardSlash[1].split(":")
                if '.' in first or ':' in first or\
                        'localhost' in first:
                    server = first
                else:
                    namespace = first
                if len(second) == 2:
                    repo = second[0]
                    tag = second[1]
                else:
                    repo = forwardSlash[1]
                    tag = 'latest'
            elif len(forwardSlash) == 3:
                server = forwardSlash[0]
                namespace = forwardSlash[1]
                split2 = forwardSlash[2].split(':')
                if len(split2) == 1:
                    repo = forwardSlash[2]
                    tag = 'latest'
                else:
                    repo = split2[0]
                    tag = split2[1]
            else:
                return None
        if namespace is not None:
            lookUpName = namespace + '/' + repo
        else:
            lookUpName = repo

        if server == "index.docker.io":
            if namespace is None:
                qualifiedName = repo
            else:
                qualifiedName = namespace + "/" + repo

        else:
            if namespace is None:
                qualifiedName = server + "/" + repo
            else:
                qualifiedName = server + "/" + namespace + "/" + repo

        return dict(repository=repo,
                    lookUpName=lookUpName,
                    server=server,
                    namespace=namespace,
                    tag=tag,
                    fullName=image,
                    qualifiedName=qualifiedName)
