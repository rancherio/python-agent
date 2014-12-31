import shlex
import struct
import six
import logging

from docker import Client

DOCKER_STREAM_STDOUT = 1
DOCKER_STREAM_STDERR = 2

STREAM_HEADER_SIZE_BYTES = 8

log = logging.getLogger('docker')


class ClientSock:
    def __init__(self, response, client, stream):
        self.response = response
        self.client = client
        self.stream = stream
        self.socket = response.raw._fp.fp._sock

    def read(self, stdout=True, stderr=True):
        res = self.socket
        if self.stream:
            return self.client._multiplexed_socket_stream_helper(res,
                                                                 stdout,
                                                                 stderr)
        else:
            return str().join(
                [x for x in self.client._multiplexed_buffer_helper(res,
                                                                   stdout,
                                                                   stderr)]
            )

    def write(self, data):
        self.socket.sendall(data)
        self.socket.sendall('\n')

    def close(self):
        self.socket.close()

    def shutdown(self, flag):
        self.socket.shutdown(flag)


class DockerClient(Client):
    def __init__(self, base_url=None, version='1.15', tls=False, timeout=60):
        super(DockerClient, self).__init__(base_url=base_url,
                                           version=version,
                                           tls=tls,
                                           timeout=timeout)

    def _multiplexed_buffer_helper(self, response, stdout=True, stderr=True):
        """A generator of multiplexed data blocks read from a buffered
        response."""
        buf = self._result(response, binary=True)
        walker = 0
        while True:
            if len(buf[walker:]) < 8:
                break
            stream_type, length = struct.unpack_from('>BxxxL', buf[walker:])
            start = walker + STREAM_HEADER_SIZE_BYTES
            end = start + length
            walker = end
            if stream_type == DOCKER_STREAM_STDOUT and not stdout:
                continue
            if stream_type == DOCKER_STREAM_STDERR and not stderr:
                continue
            yield buf[start:end]

    def _multiplexed_socket_stream_helper(self, socket, stdout=True,
                                          stderr=True):
        """A generator of multiplexed data blocks coming from a response
        socket."""
        def recvall(socket, size):
            blocks = []
            while size > 0:
                block = socket.recv(size)
                if not block:
                    return None

                blocks.append(block)
                size -= len(block)

            return ''.join(blocks)

        while True:
            socket.settimeout(None)
            header = recvall(socket, STREAM_HEADER_SIZE_BYTES)
            if not header:
                break
            stream_type, length = struct.unpack('>BxxxL', header)
            if not length:
                break
            data = recvall(socket, length)
            if not data:
                break
            yield data

    def execute(self, container, cmd, detach=False, stdout=True, stderr=True,
                stream=False, tty=False):
        cmd_id = self.executeCreate(container, cmd, detach, stdout, stderr,
                                    stream, False, tty)
        return self.executeStart(cmd_id, detach, tty, stream).read()

    def executeCreate(self, container, cmd, detach=False,
                      stdout=True, stderr=True, stream=False,
                      stdin=False, tty=False):
        if isinstance(container, dict):
            container = container.get('Id')
        if isinstance(cmd, six.string_types):
            cmd = shlex.split(str(cmd))

        data = {
            'Container': container,
            'User': '',
            'Privileged': False,
            'Tty': tty,
            'AttachStdin': stdin,
            'AttachStdout': stdout,
            'AttachStderr': stderr,
            'Detach': detach,
            'Cmd': cmd
        }

        # create the command
        url = self._url('/containers/{0}/exec'.format(container))
        res = self._post_json(url, data=data)
        self._raise_for_status(res)

        cmd_id = res.json().get('Id')
        return cmd_id

    def executeStart(self, cmd_id, detach=False, tty=False, stream=False):
        data = {
            'Detach': detach,
            'Tty': tty
        }

        res = self._post_json(self._url('/exec/{0}/start'.format(cmd_id)),
                              data=data, stream=stream)
        self._raise_for_status(res)
        return ClientSock(res, self, stream)
