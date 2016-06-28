from .common_fixtures import *  # NOQA
from .docker_common import *  # NOQA


@if_docker
def test_native_container_activate_only(agent, responses):
    # Recieving an activate event for a running, pre-existing container should
    # result in the container continuing to run and the appropriate data sent
    # back in the response (like, ports, ip, inspect, etc)
    delete_container('/native_container')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld:latest',
                                name='native_container')
    client.start(c)
    inspect = docker_client().inspect_container(c['Id'])

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['externalId'] = c['Id']

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        diff_dict(inspect, docker_inspect)
        assert docker_inspect['State']['Running']
        container_field_test_boiler_plate(resp)
        assert state_file_exists(docker_inspect['Id'])

    event_test(agent, 'docker/native_container_activate',
               pre_func=pre, post_func=post)


@if_docker
def test_native_container_activate_not_running(agent, responses):
    # Receiving an activate event for a pre-existing stopped container
    # that Rancher never recorded as having started should result in the
    # container staying stopped.
    delete_container('/native_container')

    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld:latest',
                                name='native_container')
    inspect = docker_client().inspect_container(c['Id'])

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['externalId'] = c['Id']

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        diff_dict(inspect, docker_inspect)
        assert not docker_inspect['State']['Running']
        container_field_test_boiler_plate(resp)
        assert state_file_exists(docker_inspect['Id'])

    event_test(agent, 'docker/native_container_not_running',
               pre_func=pre, post_func=post)


@if_docker
def test_native_container_activate_removed(agent, responses):
    # Receiving an activate event for a pre-existing, but removed container
    # should result in the container continuing to not exist and a valid but
    # minimally populated response.
    delete_container('/native_container')
    client = docker_client()
    c = client.create_container('ibuildthecloud/helloworld',
                                name='native_container')
    delete_container('/native_container')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['externalId'] = c['Id']

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        assert not instance_data['dockerInspect']
        assert not instance_data['dockerContainer']
        fields = instance_data['+fields']
        assert not fields['dockerIp']
        assert not fields['dockerPorts']
        assert fields['dockerHostIp']
        assert not get_container('/native_container')

    event_test(agent, 'docker/native_container_not_running',
               pre_func=pre, post_func=post, diff=False)


@if_docker
def test_native_container_deactivate_only(agent, responses):
    # TODO This test is slow bc deactivating the instance takes long
    test_native_container_activate_only(agent, responses)

    c = get_container('/native_container')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['externalId'] = c['Id']

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['State']['Running']
        assert state_file_exists(docker_inspect['Id'])
        container_field_test_boiler_plate(resp)

    event_test(agent, 'docker/native_container_deactivate',
               pre_func=pre, post_func=post)

    def pre_second_start(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['externalId'] = c['Id']
        instance['firstRunning'] = 1389656010338
        del req['data']['processData']['containerNoOpEvent']

    def post_second_start(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['State']['Running']
        assert state_file_exists(docker_inspect['Id'])
        container_field_test_boiler_plate(resp)

    event_test(agent, 'docker/native_container_activate',
               pre_func=pre_second_start, post_func=post_second_start)


@if_docker
def test_native_container_deactivate_no_op(agent, responses):
    # If a container receieves a no-op deactivate event, it should not
    # be deactivated.
    test_native_container_activate_only(agent, responses)

    c = get_container('/native_container')

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['externalId'] = c['Id']

        class Expando(object):
            pass
        req['data']['processData'] = Expando()
        req['data']['processData'].containerNoOpEvent = True

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        del instance_data['dockerContainer']['Ports'][0]
        del instance_data['+fields']['dockerPorts'][0]
        docker_inspect = instance_data['dockerInspect']
        assert docker_inspect['State']['Running']
        container_field_test_boiler_plate(resp)

    event_test(agent, 'docker/native_container_deactivate',
               pre_func=pre, post_func=post)


@if_docker
def test_native_container_activate_no_op(agent, responses):
    # If a container receieves a no-op activate event, it should not
    # be activated.
    test_native_container_activate_only(agent, responses)

    c = get_container('/native_container')
    docker_client().stop(c)

    def pre(req):
        instance = req['data']['instanceHostMap']['instance']
        instance['externalId'] = c['Id']

    def post(req, resp):
        instance_data = resp['data']['instanceHostMap']['instance']['+data']
        instance_data['dockerContainer']['Ports'].append(
            {'Type': 'tcp', 'PrivatePort': 8080})
        instance_data['+fields']['dockerPorts'].append('8080/tcp')
        docker_inspect = instance_data['dockerInspect']
        assert not docker_inspect['State']['Running']
        container_field_test_boiler_plate(resp)

    event_test(agent, 'docker/native_container_activate',
               pre_func=pre, post_func=post)


@if_docker
def test_native_container_remove(agent, responses):
    test_native_container_activate_only(agent, responses)

    c = get_container('/native_container')
    docker_client().stop(c)
    assert state_file_exists(c['Id'])

    def pre(req):
        instance = req['data']['volumeStoragePoolMap']['volume']['instance']
        instance['externalId'] = c['Id']

    def post(req, resp):
        assert not state_file_exists(c['Id'])

    event_test(agent, 'docker/native_container_volume_remove',
               pre_func=pre, post_func=post)

    # Removing a removed container. Should be error free
    event_test(agent, 'docker/native_container_volume_remove',
               pre_func=pre, post_func=post)
