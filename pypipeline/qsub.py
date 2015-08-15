'''
Created on Jan 13, 2012

@author: mgormley
'''

import datetime

def _get_willow_qsub_args(queue_list, threads, work_mem_megs, time="08:00:00"):
    args = ""
    for queue in queue_list:
        args += " -q %s " % (queue)
    return args + " -l num_proc=%d,mem_free=%dM,h_rt=%s " % (threads, work_mem_megs, time)

def _get_wisp_qsub_args(queue, threads, work_mem_megs, time="08:00:00"):
    # We used to include: "h_vmem=%dM," % (work_mem_megs)
    return " -q %s -l num_proc=%d,mem_free=%dM,h_rt=%s " % (queue, threads, work_mem_megs, time)

def _get_clsp_qsub_args(threads, work_mem_megs):
    return " -q all.q -pe smp %d -l 'arch=*amd64' -l mem_free=%dM,ram_free=%dM " % (threads, work_mem_megs, work_mem_megs)

def get_qsub_args(queue, threads, work_mem_megs, minutes):
    time = get_mins_as_hrt_str(minutes)
    if queue is not None and queue.startswith("clsp-"):
        return _get_clsp_qsub_args(threads, work_mem_megs)
    elif queue is not None and queue.startswith("wisp-"):
        return _get_wisp_qsub_args("text.q", threads, work_mem_megs, time)
    else:
        return _get_willow_qsub_args(["all.q"], threads, work_mem_megs, time)
    
def get_mins_as_hrt_str(total_mins):
    hours, minutes = divmod(total_mins, 60)
    seconds = 0
    return "%02d:%02d:%02d" % (hours, minutes, seconds)

def get_default_qsub_params(queue):
    '''Gets the default threads, work_mem_megs, and minutes for a named queue.'''
    minutes = 8*60
    if queue == "clsp-mem":
        threads = 4
        work_mem_megs = 8192
    elif queue == "clsp-cpu":
        threads = 1
        work_mem_megs = 2048
    elif queue == "cpu2x":            
        threads = 4
        work_mem_megs = 4096
    elif queue == "mem128":  
        threads = 2
        work_mem_megs = 120000
    elif queue == "himem" or queue == "wisp-himem":  
        threads = 2
        work_mem_megs = 16384
    elif queue == "mem" or queue == "wisp-mem":  
        threads = 2
        work_mem_megs = 8192
    else: # queue == "cpu"
        threads = 2
        work_mem_megs = 2048

    return (threads, work_mem_megs, minutes)
