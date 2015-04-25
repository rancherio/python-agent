import atexit
import logging
import subprocess

import os
import os.path
from cattle import Config


log = logging.getLogger("volmgr")


def register_loopback(data_file):
    # ideally we should have any leftover...
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

        global root_dir
        global mount_dir
        root_dir = Config.volmgr_root()
        mount_dir = Config.volmgr_mount_dir()
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
        if not os.path.exists(mount_dir):
            os.makedirs(mount_dir)

        global base_cmdline
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
