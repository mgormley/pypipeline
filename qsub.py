'''
Created on Jan 13, 2012

@author: mgormley
'''

def get_coe_qsub_args(queue, threads, work_mem_megs):
    return " -q %s -l num_proc=%d -l h_vmem=%dM -l virtual_free=%dM " % (queue, threads, work_mem_megs, work_mem_megs)
        
def get_wisp_qsub_args(queue, threads, work_mem_megs, time="1000:00:00"):
    work_mem_megs *= 1.5
    return " -q %s -l num_proc=%d,h_vmem=%dM,mem_free=%dM,h_rt=%s " % (queue, threads, work_mem_megs, work_mem_megs, time)

def get_clsp_qsub_args(threads, work_mem_megs):
    return " -q all.q -pe smp %d -l cpu_arch=x86_64 -l mem_free=%dM " % (threads, work_mem_megs)

def get_qsub_args(queue):
    if queue == "himem":
        threads = 6
        work_mem_megs = 32768
        qsub_args = get_coe_qsub_args("himem.q", threads, work_mem_megs)
    elif queue == "mem":
        threads = 4
        work_mem_megs = 8192
        qsub_args = get_coe_qsub_args("mem.q", threads, work_mem_megs)
    elif queue == "clsp-mem":
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
        qsub_args = get_coe_qsub_args("cpu.q", threads, work_mem_megs)
    elif queue == "cpuwisp":            
        threads = 1
        work_mem_megs = 2048
        qsub_args = get_wisp_qsub_args("all.q", threads, work_mem_megs)
    elif queue == "memwisp":  
        threads = 1
        work_mem_megs = 8192
        qsub_args = get_wisp_qsub_args("all.q", threads, work_mem_megs)
    elif queue == "himemwisp":  
        threads = 1
        work_mem_megs = 16384
        qsub_args = get_wisp_qsub_args("all.q", threads, work_mem_megs)
    else: # queue == "cpu"
        threads = 1
        work_mem_megs = 2048
        qsub_args = get_coe_qsub_args("cpu.q", threads, work_mem_megs)

    return (qsub_args, threads, work_mem_megs)
