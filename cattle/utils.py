from tempfile import NamedTemporaryFile
from os import path
from urlparse import urlparse
from contextlib import closing

import binascii
import calendar
import logging
import os
import re
import subprocess
import time
import uuid
import json
import urllib2
import arrow


try:
    from eventlet.green.subprocess import check_output as e_check_output
    from eventlet.green.subprocess import Popen as e_popen
except:
    pass

from subprocess import PIPE, Popen, CalledProcessError

log = logging.getLogger('cattle')

_TEMP_NAME = 'work'
_TEMP_PREFIX = 'cattle-temp-'


def _to_json_object(v):
    if isinstance(v, dict):
        return JsonObject(v)
    elif isinstance(v, list):
        ret = []
        for i in v:
            ret.append(_to_json_object(i))
        return ret
    else:
        return v


class JsonObject:
    def __init__(self, data):
        for k, v in data.items():
            self.__dict__[k] = _to_json_object(v)

    def __getitem__(self, item):
        value = self.__dict__[item]
        if isinstance(value, JsonObject):
            return value.__dict__
        return value

    def __getattr__(self, name):
        return getattr(self.__dict__, name)

    @staticmethod
    def unwrap(json_object):
        if isinstance(json_object, list):
            ret = []
            for i in json_object:
                ret.append(JsonObject.unwrap(i))
            return ret

        if isinstance(json_object, dict):
            ret = {}
            for k, v in json_object.items():
                ret[k] = JsonObject.unwrap(v)
            return ret

        if isinstance(json_object, JsonObject):
            ret = {}
            for k, v in json_object.__dict__.items():
                ret[k] = JsonObject.unwrap(v)
            return ret

        return json_object


class CadvisorAPIClient(object):
    def __init__(self, host, port, version='v1.2', proto='http://'):
        self.url = '{0}{1}:{2}/api/{3}'.format(proto, host, str(port), version)

    def get_containers(self):
        return self._get(self.url + '/containers')

    def get_latest_stat(self):
        containers = self.get_stats()
        if containers is not None:
            if len(containers) > 1:
                return containers[-1]
        return {}

    def get_stats(self):
        containers = self.get_containers()
        if containers is not None:
            return containers['stats']
        return []

    def timestamp_diff(self, time_current, time_prev):
        time_current_conv = self._timestamp_convert(time_current)
        time_prev_conv = self._timestamp_convert(time_prev)

        diff = (time_current_conv - time_prev_conv).total_seconds()
        return round((diff * 10**9))

    def _timestamp_convert(self, stime):
        # Cadvisor handles everything in nanoseconds.
        # Python does not.
        t_conv = arrow.get(stime[0:26])
        return t_conv

    def _marshall_to_python(self, data):
        if isinstance(data, str):
            return json.loads(data)

    def _get(self, url):
        try:
            with closing(urllib2.urlopen(url, timeout=5)) as resp:
                if resp.code == 200:
                    data = resp.read()
                    return self._marshall_to_python(data)
        except:
            log.exception(
                "Could not get stats from cAdvisor at: {0}".format(url))

        return None


def ping_include_resources(ping):
    try:
        return ping.data.options['resources']
    except (KeyError, AttributeError):
        return False


def ping_include_stats(ping):
    try:
        return ping.data.options['stats']
    except (KeyError, AttributeError):
        return False


def ping_include_instances(ping):
    try:
        return ping.data.options['instances']
    except (KeyError, AttributeError):
        return False


def ping_add_resources(pong, *args):
    if 'resources' not in pong.data:
        pong.data.resources = []

    for resource in args:
        pong.data.resources.append(resource)


def ping_set_option(pong, key, value):
    if 'options' not in pong.data:
        pong.data.options = {}

    pong.data.options[key] = value


def events_from_methods(obj):
    ret = []
    for method in dir(obj):
        if method.startswith('_do_'):
            ret.append(method[4:].replace('_', '.'))
    return ret


def reply(event, data=None, parent=None):
    if data is None:
        data = JsonObject({})

    result = None
    if event is not None and event.replyTo is not None:
        result = _reply_obj(event, data)

    if parent is not None:
        if parent.replyTo is None:
            return None
        else:
            return _reply_obj(parent, result)

    return result


def _reply_obj(event, data):
    return JsonObject({
        'id': str(uuid.uuid4()),
        'name': event.replyTo,
        'data': data,
        'resourceType': event.resourceType,
        'resourceId': event.resourceId,
        'previousIds': [event.id],
        'previousNames': [event.name],
        'time': calendar.timegm(time.gmtime()) * 1000,
    })


def get_data(obj, prefix=None, strip_prefix=True):
    result = {}

    if obj is None:
        return result

    data = obj.get('data')
    if data is None:
        return result

    for k, v in data.items():
        if prefix is None:
            result[k] = v
            continue

        if not k.startswith(prefix):
            continue

        if strip_prefix:
            k = k[len(prefix)+1:]

        result[k] = v

    return result


def memoize(function):
    memo = {}

    def wrapper(*args):
        if args in memo:
            return memo[args]
        else:
            rv = function(*args)
            memo[args] = rv
            return rv
    return wrapper


def get_map_value(obj, *args):
    current = obj
    for arg in args:
        child = current.get(arg)
        if child is None:
            child = {}
            current[arg] = child
        current = child

    return current


def temp_file(destination):
    temp_dst = NamedTemporaryFile(prefix=_TEMP_PREFIX, dir=destination)
    temp_dst.close()

    return temp_dst.name


def temp_file_in_work_dir(destination):
    dst_path = path.join(destination, _TEMP_NAME)
    if not path.exists(dst_path):
        os.makedirs(dst_path)

    return temp_file(dst_path)


def get_command_output(*args, **kw):
    try:
        kw['stderr'] = subprocess.STDOUT
        return check_output(*args, **kw)
    except subprocess.CalledProcessError as e:
        if not (e.output == 'Lock failed' and e.returncode == 122):
            log.exception('Failed to call %s %s, exit [%s], output :\n%s',
                          args, kw, e.returncode, e.output)
        raise e


def _check_output(*popenargs, **kwargs):
    # Copyright (c) 2003-2005 by Peter Astrand <astrand@lysator.liu.se>
    #
    # Licensed to PSF under a Contributor Agreement.
    # See http://www.python.org/2.4/license for licensing details.
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = Popen(stdout=PIPE, *popenargs, **kwargs)
    output, _ = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        e = CalledProcessError(retcode, cmd)
        e.output = output
        raise e
    return output


_check_output_impl = None


def check_output(*popenargs, **kwargs):
    global _check_output_impl
    if _check_output_impl is None:
        from cattle import Config
        if Config.is_eventlet() and 'e_check_output' in globals():
            _check_output_impl = e_check_output
        else:
            try:
                import subprocess32
                _check_output_impl = subprocess32.check_output
            except:
                if 'check_output' in dir(subprocess):
                    _check_output_impl = subprocess.check_output
                else:
                    _check_output_impl = _check_output

    try:
        return _check_output_impl(*popenargs, **kwargs)
    except subprocess.CalledProcessError as e:
        raise e
    except Exception as e:
        # eventlets seems to throw a CalledProcessError that isn't the same
        # as the subprocess package exception
        try:
            raise subprocess.CalledProcessError(e.returncode, e.cmd, e.output)
        except subprocess.CalledProcessError as e1:
            raise e1
        except:
            # This is in case CallProcessError doesn't have returncode, cmd,
            # or output
            raise e


def popen(*args, **kw):
    try:
        return e_popen(*args, **kw)
    except NameError:
        return Popen(*args, **kw)


def random_string(length=64):
    return binascii.hexlify(os.urandom(length/2))


def is_uuid(str):
    if str is None:
        return False
    return re.match(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
                    r'-[0-9a-f]{4}-[0-9a-f]{12}', str) is not None


def get_url_port(url):
    parsed = urlparse(url)

    port = parsed.port

    if port is None:
        if parsed.scheme == 'http':
            port = 80
        elif parsed.scheme == 'https':
            port = 443

    if port is None:
        raise Exception('Failed to find port for {0}'.format(url))

    return port


def get_or_create_map(container, key):
    map = container.get(key)
    if map is None:
        map = {}
        container[key] = map

    return map


def get_or_create_list(container, key):
    value = container.get(key)
    if value is None:
        value = []
        container[key] = value

    return value


def log_request(req, log, *args):
    debug = False
    try:
        if 'ping' in req.name:
            debug = True
    except:
        pass

    try:
        if 'ping' in req.data.event.name:
            debug = True
    except:
        pass

    try:
        if 'ping' in req.previousNames[0]:
            debug = True
    except:
        pass

    try:
        if 'ping' in req.data.previousNames[0]:
            debug = True
    except:
        pass

    if debug:
        log.debug(*args)
    else:
        log.info(*args)
