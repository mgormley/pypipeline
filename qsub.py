'''
Created on Jan 13, 2012

@author: mgormley
'''

def get_wisp_qsub_args(queue, threads, work_mem_megs, time="8:00:00"):
    # We used to include: "h_vmem=%dM," % (work_mem_megs)
    return " -q %s -l num_proc=%d,mem_free=%dM,h_rt=%s " % (queue, threads, work_mem_megs, time)

def get_clsp_qsub_args(threads, work_mem_megs):
    return " -q all.q -pe smp %d -l cpu_arch=x86_64 -l mem_free=%dM " % (threads, work_mem_megs)

def get_qsub_args(queue):
    if queue == "clsp-mem":
        threads = 4
        work_mem_megs = 8192
        qsub_args = get_clsp_qsub_args(threads, work_mem_megs)
    elif queue == "clsp-cpu":
        threads = 1
        work_mem_megs = 2048
        qsub_args = get_clsp_qsub_args(threads, work_mem_megs)
    elif queue == "cpu2x":            
        threads = 2
        work_mem_megs = 4096
        qsub_args = get_wisp_qsub_args("all.q", threads, work_mem_megs)
    elif queue == "himem":  
        threads = 1
        work_mem_megs = 16384
        qsub_args = get_wisp_qsub_args("all.q", threads, work_mem_megs)
    elif queue == "mem":  
        threads = 1
        work_mem_megs = 8192
        qsub_args = get_wisp_qsub_args("all.q", threads, work_mem_megs)
    else: # queue == "cpu"
        threads = 1
        work_mem_megs = 2048
        qsub_args = get_wisp_qsub_args("all.q", threads, work_mem_megs)

    return (qsub_args, threads, work_mem_megs)
