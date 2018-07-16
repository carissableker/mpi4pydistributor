#! /usr/bin/env python

import mpi4pydistributor as mpi

def f(x):
    return x*100

if mpi.rank == 0:
    # make tasks
    tasks = range(10)
    results = mpi.master_task_distributer(tasks) 
    print(results)   

else:
    mpi.worker_task_receiver(f)

