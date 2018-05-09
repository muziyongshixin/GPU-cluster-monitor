from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import os
import psutil
import py3nvml.py3nvml as N
import json
import platform
import socket

NOT_SUPPORTED = 'Not Supported'

class GPUStat(object):

    def __init__(self, entry):
        if not isinstance(entry, dict):
            raise TypeError('entry should be a dict, {} given'.format(type(entry)))
        self.entry = entry

        for k in self.entry.keys():
            if isinstance(self.entry[k], six.string_types) and NOT_SUPPORTED in self.entry[k]:
                self.entry[k] = None

    def keys(self):
        return self.entry.keys()

    def __getitem__(self, key):
        return self.entry[key]

    @property
    def index(self):
        """
        Returns the index of GPU (as in nvidia-smi).
        """
        return self.entry['index']

    @property
    def uuid(self):
        """
        Returns the uuid returned by nvidia-smi,
        e.g. GPU-12345678-abcd-abcd-uuid-123456abcdef
        """
        return self.entry['uuid']

    @property
    def name(self):
        """
        Returns the name of GPU card (e.g. Geforce Titan X)
        """
        return self.entry['name']

    @property
    def memory_total(self):
        """
        Returns the total memory (in MB) as an integer.
        """
        return int(self.entry['memory.total'])

    @property
    def memory_used(self):
        """
        Returns the occupied memory (in MB) as an integer.
        """
        return int(self.entry['memory.used'])

    @property
    def memory_free(self):
        """
        Returns the free (available) memory (in MB) as an integer.
        """
        v = self.memory_total - self.memory_used
        return max(v, 0)

    @property
    def memory_available(self):
        """
        Returns the available memory (in MB) as an integer. Alias of memory_free.
        """
        return self.memory_free

    @property
    def temperature(self):
        """
        Returns the temperature of GPU as an integer,
        or None if the information is not available.
        """
        v = self.entry['temperature.gpu']
        return int(v) if v is not None else None

    @property
    def utilization(self):
        """
        Returns the GPU utilization (in percentile),
        or None if the information is not available.
        """
        v = self.entry['utilization.gpu']
        return int(v) if v is not None else None

    @property
    def power_draw(self):
        """
        Returns the GPU power usage in Watts,
        or None if the information is not available.
        """
        v = self.entry['power.draw']
        return int(v) if v is not None else None

    @property
    def power_limit(self):
        """
        Returns the (enforced) GPU power limit in Watts,
        or None if the information is not available.
        """
        v = self.entry['enforced.power.limit']
        return int(v) if v is not None else None

    @property
    def processes(self):
        """
        Get the list of running processes on the GPU.
        """
        return list(self.entry['processes'])

def new_query():
    """Query the information of all the GPUs on local machine"""

    N.nvmlInit()

    def get_gpu_info(handle):
        """Get one GPU information specified by nvml handle"""

        def get_process_info(nv_process):
            """Get the process information of specific pid"""
            process = {}
            ps_process = psutil.Process(pid=nv_process.pid)
            process['username'] = ps_process.username()
            # cmdline returns full path; as in `ps -o comm`, get short cmdnames.
            _cmdline = ps_process.cmdline()
            if not _cmdline:  # sometimes, zombie or unknown (e.g. [kworker/8:2H])
                process['command'] = '?'
            else:
                process['command'] = os.path.basename(_cmdline[0])
            # Bytes to MBytes
            process['gpu_memory_usage'] = int(nv_process.usedGpuMemory / 1024 / 1024)
            process['pid'] = nv_process.pid
            return process

        def _decode(b):
            if isinstance(b, bytes):
                return b.decode()  # for python3, to unicode
            return b

        name = _decode(N.nvmlDeviceGetName(handle))
        uuid = _decode(N.nvmlDeviceGetUUID(handle))

        try:
            temperature = N.nvmlDeviceGetTemperature(handle, N.NVML_TEMPERATURE_GPU)
        except N.NVMLError:
            temperature = None  # Not supported

        try:
            memory = N.nvmlDeviceGetMemoryInfo(handle)  # in Bytes
        except N.NVMLError:
            memory = None  # Not supported

        try:
            utilization = N.nvmlDeviceGetUtilizationRates(handle)
        except N.NVMLError:
            utilization = None  # Not supported

        try:
            power = N.nvmlDeviceGetPowerUsage(handle)
        except:
            power = None

        try:
            power_limit = N.nvmlDeviceGetEnforcedPowerLimit(handle)
        except:
            power_limit = None

        processes = []
        try:
            nv_comp_processes = N.nvmlDeviceGetComputeRunningProcesses(handle)
        except N.NVMLError:
            nv_comp_processes = None  # Not supported
        try:
            nv_graphics_processes = N.nvmlDeviceGetGraphicsRunningProcesses(handle)
        except N.NVMLError:
            nv_graphics_processes = None  # Not supported

        if nv_comp_processes is None and nv_graphics_processes is None:
            processes = None  # Not supported (in both cases)
        else:
            nv_comp_processes = nv_comp_processes or []
            nv_graphics_processes = nv_graphics_processes or []
            for nv_process in (nv_comp_processes + nv_graphics_processes):
                # TODO: could be more information such as system memory usage,
                # CPU percentage, create time etc.
                try:
                    process = get_process_info(nv_process)
                    processes.append(process)
                except psutil.NoSuchProcess:
                    # TODO: add some reminder for NVML broken context
                    # e.g. nvidia-smi reset  or  reboot the system
                    pass

        index = N.nvmlDeviceGetIndex(handle)
        gpu_info = {
            'index': index,
            'uuid': uuid,
            'name': name,
            'temperature.gpu': temperature,
            'utilization.gpu': utilization.gpu if utilization else None,
            'power.draw': int(power / 1000) if power is not None else None,
            'enforced.power.limit': int(power_limit / 1000) if power_limit is not None else None,
            # Convert bytes into MBytes
            'memory.used': int(memory.used / 1024 / 1024) if memory else None,
            'memory.total': int(memory.total / 1024 / 1024) if memory else None,
            'processes': processes,
        }
        return gpu_info

    # 1. get the list of gpu and status
    gpu_list = {}
    device_count = N.nvmlDeviceGetCount()

    for index in range(device_count):
        handle = N.nvmlDeviceGetHandleByIndex(index)
        gpu_info = get_gpu_info(handle)
        # gpu_stat = GPUStat(gpu_info)
        gpu_list[index] = gpu_info

    N.nvmlShutdown()
    return gpu_list

def print_info_for_test(info):
    for gpu_id, gpu_info in info.items():
        print ("gpu_id is " + str(gpu_id))
        for k, v in gpu_info.items():
            if k != "processes":
                print (str(k) + " : " + str(v))
            else:
                print ("\nprocesses info\n")
                for process in v:
                    for kk, vv in process.items():
                        print (str(kk) + " : " + str(vv))

def main():
    info = new_query()
    hostname = socket.gethostname()
    # print_info_for_test(info)
    DIR="/home/liyz/icst0/clusterThreadCount/"
    filename = DIR+str(hostname) + ".json"
    with open(filename, "w") as f:
        json.dump(info, f)

if __name__ == "__main__":
    main()
