import atexit
import logging
import subprocess

import os
import os.path
from cattle import Config
from .volmgr_service import VolmgrService


log = logging.getLogger("volmgr")

RANCHER_PREFIX = "/rancher/"
INSTANCE_TAG_FILE = "instance"

v = VolmgrService("")
blockstore_uuid = ""


def register_loopback(data_file):
    # ideally we shouldn't have any leftover...
    output = subprocess.check_output(["losetup", "-j", data_file])
    if output == "":
        output = subprocess.check_output(["losetup", "-v", "-f",
                                          data_file])
        data_dev = output.strip().split(" ")[3]
    else:
        data_dev = output.split(":")[0].strip()
    atexit.register(cleanup_loopback, data_dev)
    assert data_dev.startswith("/dev/loop")
    return data_dev


def cleanup_loopback(dev):
    subprocess.call(["losetup", "-d", dev])


def get_volume_dir(vol_name, user):
    return os.path.join(Config.volmgr_mount_dir(), user, vol_name)


def mounted(mount_dir):
    output = subprocess.check_output(["mount"])
    if output.find(mount_dir):
        return True
    return False


def get_volume_uuid(path):
    filelist = os.listdir(path)
    volume_uuid = ""
    for i in filelist:
        if i == INSTANCE_TAG_FILE:
            continue
        volume_uuid = i
    return volume_uuid


def get_volume(vol_name, vol_size, instance_name, user):
    path = get_volume_dir(vol_name, user)
    if os.path.exists(path):
        volume_uuid = get_volume_uuid(path)
        create = False
        if volume_uuid == "":
            log.warning("Found volume directory but cannot find related \
                    volume! Create one")
            create = True

        old_instance_file = os.open(os.path.join(path, INSTANCE_TAG_FILE), "r")
        old_instance_name = ""
        try:
            old_instance_name = old_instance_file.read()
        finally:
            old_instance_file.close()
        assert old_instance_name == instance_name

        if not create:
            mount_dir = os.path.join(path, volume_uuid)
            if not mounted(mount_dir):
                v.mount_volume(volume_uuid, mount_dir, False)
            return mount_dir

    volume_uuid = v.create_volume(vol_size)
    v.add_volume_to_blockstore(volume_uuid, blockstore_uuid)
    mount_dir = os.path.join(path, volume_uuid)
    os.makedirs(mount_dir)
    f = open(os.path.join(path, INSTANCE_TAG_FILE), "w")
    try:
        f.write(instance_name)
    finally:
        f.close()
    v.mount_volume(volume_uuid, mount_dir, True)
    return mount_dir


def create_snapshot(vol_uuid):
    return v.create_snapshot(vol_uuid)


def backup_snapshot(snapshot_uuid, vol_uuid):
    v.backup_snapshot_to_blockstore(snapshot_uuid, vol_uuid, blockstore_uuid)


def remove_snapshot_from_blockstore(snapshot_uuid, vol_uuid):
    v.remove_snapshot_from_blockstore(snapshot_uuid, vol_uuid,
                                      blockstore_uuid)


def delete_snapshot(snapshot_uuid, vol_uuid):
    v.delete_snapshot(snapshot_uuid, vol_uuid)


def restore_snapshot(vol_name, old_vol_name, vol_size,
                     snapshot_uuid, instance_name, user):
    path = get_volume_dir(vol_name, user)
    if os.path.exists(path):
        log.info("Already found the volume, skip restore")
        volume_uuid = get_volume_uuid(path)
        return os.path.join(path, volume_uuid)
    old_path = get_volume_dir(old_vol_name, user)
    if not os.path.exists(old_path):
        raise Exception("Cannot find old volume")
    old_volume_uuid = get_volume_uuid(old_path)
    volume_uuid = v.create_volume(vol_size)
    v.restore_snapshot_from_blockstore(snapshot_uuid, old_volume_uuid,
                                       volume_uuid, blockstore_uuid)
    v.add_volume_to_blockstore(volume_uuid, blockstore_uuid)

    mount_dir = os.path.join(path, volume_uuid)
    os.makedirs(mount_dir)
    f = open(os.path.join(path, INSTANCE_TAG_FILE), "w")
    try:
        f.write(instance_name)
    finally:
        f.close()
    v.mount_volume(volume_uuid, mount_dir, False)
    return mount_dir


def update_managed_volume(instance, config, start_config):
    if 'binds' not in start_config:
        return
    binds_map = start_config['binds']
    instance_name = config['name']
    new_binds_map = {}
    user = "default"
    if 'user' in config and config['user'] is not None:
        user = config['user']
    for bind in binds_map:
        src = bind
        dst = binds_map[bind]
        if src.startswith(RANCHER_PREFIX):
            vol_command = src[len(RANCHER_PREFIX):]
            words = vol_command.split("/")
            vol_name = words[0]
            command = ""
            old_vol_name = ""
            snapshot_uuid = ""
            if len(words) > 1:
                command = words[1]
                if command == "restore":
                    assert len(words) == 4
                    old_vol_name = words[2]
                    snapshot_uuid = words[3]
                else:
                    log.error("unsupported command %s, \
                              ignore and create volume", command)

            if command == "restore":
                log.info("About to restore snapshot")
                mount_point = restore_snapshot(
                    vol_name, old_vol_name,
                    Config.volmgr_default_volume_size(), snapshot_uuid,
                    instance_name, user)
                new_binds_map[mount_point] = dst
            else:
                mount_point = get_volume(vol_name,
                                         Config.volmgr_default_volume_size(),
                                         instance_name, user)
                new_binds_map[mount_point] = dst
        else:
            new_binds_map[src] = binds_map[src]
    start_config['binds'] = new_binds_map


class Volmgr(object):
    def on_startup(self):
        driver = Config.volmgr_storage_driver()
        if driver != "devicemapper":
            raise Exception("Unknown volmgr driver %s" % (driver))

        data_dev = Config.volmgr_dm_data_device()
        metadata_dev = Config.volmgr_dm_metadata_device()
        if data_dev != "" and metadata_dev != "":
            if os.path.exists(data_dev) and os.path.exists(metadata_dev):
                log.warning("Provided data_dev %s and metadata_dev %s, \
                             but unable to find the devices, \
                             continue with default files",
                            data_dev, metadata_dev)
                data_dev = ""
                metadata_dev = ""

        data_file = Config.volmgr_dm_data_file()
        metadata_file = Config.volmgr_dm_metadata_file()

        if os.path.exists(data_file) and os.path.exists(metadata_file):
            log.debug("Found existed device mapper data and metadata file, \
                    load them to loopback device")
        elif not (os.path.exists(data_file) or os.path.exists(metadata_file)):
            log.debug("Existed device mapper data and metadata file not found, \
                    create them")
            subprocess.check_call(["truncate", "-s", "100G", data_file])
            subprocess.check_call(["truncate", "-s", "5G", metadata_file])
        else:
            raise Exception("Only one of data or metadata file exists, please clean \
                    up %s ,%s" % (data_file, metadata_file))

        if data_dev == "":
            data_dev = register_loopback(data_file)
            metadata_dev = register_loopback(metadata_file)

        root_dir = Config.volmgr_root()
        mount_dir = Config.volmgr_mount_dir()
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
        if not os.path.exists(mount_dir):
            os.makedirs(mount_dir)

        base_cmdline = ["volmgr", "--debug",
                        "--log", Config.volmgr_log_file(),
                        "--root", root_dir]
        pool_name = Config.volmgr_pool_name()
        # TODO better for volmgr to verify cfg is the same
        if not os.path.exists(os.path.join(root_dir, "volmgr.cfg")):
            subprocess.check_call(base_cmdline + [
                "init",
                "--driver", driver,
                "--driver-opts", "dm.datadev=" + data_dev,
                "--driver-opts", "dm.metadatadev=" + metadata_dev,
                "--driver-opts", "dm.thinpoolname=" + pool_name])
        global v
        v = VolmgrService(base_cmdline)

        global blockstore_uuid
        if not os.path.exists(Config.volmgr_blockstore_dir()):
            os.makedirs(Config.volmgr_blockstore_dir())
        blockstore_uuid = v.register_vfs_blockstore(
            Config.volmgr_blockstore_dir())
