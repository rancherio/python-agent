#!/usr/bin/python

import subprocess
import json

import os


EXT4_FS = "ext4"


class VolmgrService(object):
    def __init__(self, cmdline):
        self.base_cmdline = cmdline

    def create_volume(self, size):
        data = subprocess.check_output(self.base_cmdline + [
            "volume", "create",
            "--size", str(size)])
        volume = json.loads(data)
        uuid = volume["UUID"]
        assert volume["Size"] == size
        return uuid

    def delete_volume(self, uuid):
        subprocess.check_call(self.base_cmdline + ["volume", "delete",
                                                   "--uuid", uuid])

    def mount_volume(self, uuid, path, need_format):
        assert os.path.exists(path)
        cmdline = self.base_cmdline + ["volume", "mount",
                                       "--switch-ns", "/host/proc/1/ns/mnt",
                                       "--uuid", uuid,
                                       "--mountpoint", path,
                                       "--fs", EXT4_FS]
        if need_format:
            cmdline = cmdline + ["--format"]

        subprocess.check_call(cmdline)

    def umount_volume(self, uuid):
        cmdline = self.base_cmdline + ["volume", "umount",
                                       "--switch-ns", "/host/proc/1/ns/mnt",
                                       "--uuid", uuid]
        subprocess.check_call(cmdline)

    def list_volumes(self, uuid=None):
        if uuid is None:
            data = subprocess.check_output(self.base_cmdline +
                                           ["volume", "list"])
            volumes = json.loads(data)
            return volumes

        data = subprocess.check_output(self.base_cmdline + ["volume", "list",
                                                            "--uuid", uuid])
        volumes = json.loads(data)
        return volumes

    def create_snapshot(self, volume_uuid):
        data = subprocess.check_output(self.base_cmdline +
                                       ["snapshot", "create",
                                        "--volume-uuid", volume_uuid])
        snapshot = json.loads(data)
        assert snapshot["VolumeUUID"] == volume_uuid
        return snapshot["UUID"]

    def delete_snapshot(self, snapshot_uuid, volume_uuid):
        subprocess.check_call(self.base_cmdline +
                              ["snapshot", "delete",
                               "--uuid", snapshot_uuid,
                               "--volume-uuid", volume_uuid])

    def register_vfs_blockstore(self, path):
        data = subprocess.check_output(self.base_cmdline +
                                       ["blockstore",
                                        "register", "--kind", "vfs",
                                        "--opts", "vfs.path=" + path])
        bs = json.loads(data)
        assert bs["Kind"] == "vfs"
        return bs["UUID"]

    def deregister_blockstore(self, uuid):
        subprocess.check_call(self.base_cmdline + ["blockstore", "deregister",
                                                   "--uuid", uuid])

    def add_volume_to_blockstore(self, volume_uuid, bs_uuid):
        subprocess.check_call(self.base_cmdline +
                              ["blockstore", "add",
                               "--volume-uuid", volume_uuid,
                               "--uuid", bs_uuid])

    def remove_volume_from_blockstore(self, volume_uuid, bs_uuid):
        subprocess.check_call(self.base_cmdline +
                              ["blockstore", "remove",
                               "--volume-uuid", volume_uuid,
                               "--uuid", bs_uuid])

    def backup_snapshot_to_blockstore(self, snapshot_uuid, volume_uuid,
                                      bs_uuid):
        subprocess.check_call(self.base_cmdline +
                              ["snapshot", "backup",
                               "--uuid", snapshot_uuid,
                               "--volume-uuid", volume_uuid,
                               "--blockstore-uuid", bs_uuid])

    def restore_snapshot_from_blockstore(self, snapshot_uuid,
                                         origin_volume_uuid,
                                         target_volume_uuid,
                                         bs_uuid):
        subprocess.check_call(self.base_cmdline +
                              ["snapshot", "restore",
                               "--uuid", snapshot_uuid,
                               "--origin-volume-uuid", origin_volume_uuid,
                               "--target-volume-uuid", target_volume_uuid,
                               "--blockstore-uuid", bs_uuid])

    def remove_snapshot_from_blockstore(self,
                                        snapshot_uuid, volume_uuid, bs_uuid):
        subprocess.check_call(self.base_cmdline +
                              ["snapshot", "remove",
                               "--uuid", snapshot_uuid,
                               "--volume-uuid", volume_uuid,
                               "--blockstore-uuid", bs_uuid])
