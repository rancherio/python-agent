from cattle.plugins.docker.storage import ImageValidationError
from docker.errors import APIError
from .common_fixtures import *  # NOQA
from .docker_common import *  # NOQA


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

    event_test(agent, 'docker/image_activate')


@if_docker
def test_instance_activate_need_pull_image(agent, responses):
    try:
        docker_client().remove_image('ibuildthecloud/helloworld:latest')
    except APIError:
        pass

    instance_only_activate(agent, responses)


@if_docker
def test_image_activate_no_reg_cred_pull_image(agent, responses):
    try:
        docker_client().remove_image('ibuildthecloud/helloworld:latest')
    except APIError:
        pass

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        image['registryCredential'] = None

    event_test(agent, 'docker/image_activate', pre_func=pre)


def _pull_image_by_name(agent, responses, image_name):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    try:
        docker_client().remove_image(image_name, noprune=True)
    except APIError:
        pass

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        remap_dockerImage(image, image_name)

    event_test(agent, 'docker/image_activate', pre_func=pre, diff=False)


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
        'cirros:0.3.3',
        'docker.io/tianon/true',
        'docker.io/library/cirros',
        'docker.io/cirros',
        'index.docker.io/tianon/true',
        'index.docker.io/library/cirros',
        'index.docker.io/cirros',
        'rocket.chat',
        'rocket.chat:latest',
        'docker.io/rocket.chat',
        'index.docker.io/rocket.chat',
        'index.docker.io/rocket.chat:latest'
    ]

    for i in image_names:
        _pull_image_by_name(agent, responses, i)


@if_docker
def _test_image_pull_credential(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
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
               diff=False)


@if_docker
def test_image_pull_invalid_credential(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    image_name = 'tianon/true'

    try:
        docker_client().remove_image(image_name)
    except APIError:
        pass

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        remap_dockerImage(image, image_name)
        image['registryCredential'] = {
            'publicValue': random_str(),
            'secretValue': random_str(),
            'data': {
                'fields': {
                    'email': 'test@rancher.com',
                }
            },
            'registry': {
                'data': {
                    'fields': {
                        'serverAddress': 'index.docker.io'
                    }
                }
            }
        }
    req = json_data('docker/image_activate')
    pre(req)
    if newer_than('1.22'):
        error_class = APIError
    else:
        error_class = ImageValidationError
    with pytest.raises(error_class) as e:
        agent.execute(req)
    assert 'auth' in str(e.value.message).lower()


@if_docker
def test_image_pull_invalid_image(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    image_name = random_str() + random_str()

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        remap_dockerImage(image, image_name)
    req = json_data('docker/image_activate')
    pre(req)
    with pytest.raises(ImageValidationError) as e:
        agent.execute(req)
    assert 'not found' in e.value.message


@if_docker
def _test_instance_pull_credential(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
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
               diff=False)


@if_docker
def test_image_activate_no_op(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
    repo = 'ubuntu'
    tag = '10.04'
    image_name = repo + ':' + tag
    client = docker_client()
    try:
        client.remove_image(image_name)
    except APIError:
        pass

    def pre(req):
        image = req['data']['imageStoragePoolMap']['image']
        remap_dockerImage(image, image_name)

        class Expando(object):
            pass
        req['data']['processData'] = Expando()
        req['data']['processData'].containerNoOpEvent = True

    def post(req, resp):
        images = client.images(name=repo)
        for i in images:
            for t in i['RepoTags']:
                assert tag not in t
        assert not resp['data']['imageStoragePoolMap']

    event_test(agent, 'docker/image_activate', pre_func=pre,
               post_func=post, diff=False)


def image_pull_invalid_credential(agent, responses):
    delete_container('/c861f990-4472-4fa1-960f-65171b544c28')
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
                        'localhost' in first\
                        and first != 'docker.io':
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

        if server == "index.docker.io" or server == "docker.io":
            if namespace is None:
                qualifiedName = repo
            else:
                qualifiedName = namespace + "/" + repo

        else:
            if namespace is None:
                qualifiedName = server + "/" + repo
            else:
                qualifiedName = server + "/" + namespace + "/" + repo
        if server == "docker.io":
            server = "index.docker.io"

        return dict(repository=repo,
                    lookUpName=lookUpName,
                    server=server,
                    namespace=namespace,
                    tag=tag,
                    fullName=image,
                    qualifiedName=qualifiedName)
