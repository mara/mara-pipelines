"""Generation of system statistics events (cpu, io, net, ram)"""

import datetime
import multiprocessing
import time

from .. import config
from ..logging import events


class SystemStatistics(events.Event):
    def __init__(self, timestamp: datetime.datetime, *, disc_read: float = None, disc_write: float = None,
                 net_recv: float = None, net_sent: float = None,
                 cpu_usage: float = None, mem_usage: float = None, swap_usage: float = None,
                 iowait: float = None) -> None:
        """
        Statistics about the system which runs the pipeline

        Individual statistics can be None

        Args:
            timestamp: The time when the statistics where gathered
            disc_read: read IO for discs in MB/s (summed)
            disc_write: write IO for discs in MB/s (summed)
            net_recv: read IO on all network adapters in MB/s (summed)
            net_sent: write IO on all network adapters in MB/s (summed)
            cpu_usage: cpu load on all cores in percent (summed)
            mem_usage: RAM used in percent of total ram
            swap_usage: swap used in percent of total swap
            iowait: How much time the CPU spends waiting for IO
        """
        super().__init__()
        self.timestamp = timestamp
        self.disc_read = disc_read
        self.disc_write = disc_write
        self.net_recv = net_recv
        self.net_sent = net_sent
        self.cpu_usage = cpu_usage
        self.mem_usage = mem_usage
        self.swap_usage = swap_usage
        self.iowait = iowait


def generate_system_statistics(event_queue: multiprocessing.Queue) -> None:
    """
    Generates one SystemStatistics event per configurable period and puts them in to a queue

    Ideas from
    http://off-the-stack.moorman.nu/2013-09-28-gather-metrics-using-psutil.html
    https://github.com/giampaolo/psutil/tree/master/scripts

    :param event_queue: The queue to write the events to
    """
    import psutil

    def cpu_usage():
        cpu_times = psutil.cpu_times_percent()
        return cpu_times.user + cpu_times.system

    def mem_usage():
        mem = psutil.virtual_memory()
        return 100.0 * mem.used / mem.total

    def swap_usage():
        swap = psutil.swap_memory()
        return 100.0 * swap.used / swap.total if swap.total > 0 else None

    # immediately send event for current cpu, mem and swap usage
    event_queue.put(SystemStatistics(
        datetime.datetime.now(), cpu_usage=cpu_usage(), mem_usage=mem_usage(), swap_usage=swap_usage()))
    period = config.system_statistics_collection_period()

    n = 0

    # capture current disc and net state for later diff
    discs_last = psutil.disk_io_counters()
    nets_last = psutil.net_io_counters()
    mb = 1024 * 1024
    time.sleep(period)
    while True:
        discs_cur = psutil.disk_io_counters()
        nets_cur = psutil.net_io_counters()
        event_queue.put(SystemStatistics(
            datetime.datetime.now(),
            disc_read=(discs_cur.read_bytes - discs_last.read_bytes) / mb / period,
            disc_write=(discs_cur.write_bytes - discs_last.write_bytes) / mb / period,
            net_recv=(nets_cur.bytes_recv - nets_last.bytes_recv) / mb / period,
            net_sent=(nets_cur.bytes_sent - nets_last.bytes_sent) / mb / period,
            cpu_usage=cpu_usage(), mem_usage=mem_usage(), swap_usage=swap_usage()))
        nets_last = nets_cur
        discs_last = discs_cur

        # double period every 100 measurements in order to avoid sending too many requests to frontend
        n += 1
        if n % 100 == 0:
            period *= 2

        time.sleep(period)
