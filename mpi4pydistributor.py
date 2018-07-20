'''
Based on http://stackoverflow.com/questions/21088420/mpi4py-send-recv-with-tag
Notes:
    stdout is clobbered
    tasks is a fixed iterable
    No cleanup, so if somehing breaks it may hang

Add to main program:
import mpi4pydistributor as mpi
'''

#------------------------------------------------------------------------------
# Imported modules: 

from mpi4py import MPI

#------------------------------------------------------------------------------
# Initializations and preliminaries
comm = MPI.COMM_WORLD               # get MPI communicator object
size = comm.Get_size()              # total number of processes
rank = comm.Get_rank()              # rank of this process
name = MPI.Get_processor_name()
status = MPI.Status()               # get MPI status object

#------------------------------------------------------------------------------
def enum(*sequential, **named):
    ''' Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    '''
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START')

#------------------------------------------------------------------------------
def master_task_distributer(tasks, function=lambda x: x, args=[], leave=0):
    '''
    Master process executesthis code. 
    `tasks`         iterables of tasks for worker processes (e.g. input file names)
    `function`      a function the master executes after recieving each result from a worker 
    `args`          a list/dictionary of arguments to `function`
    '''

    if not len(tasks):
        # tasks is not iterable
        sys.exit()

    task_index = 0
    num_workers = size - 1 - leave
    closed_workers = 0
    all_results = []

    print '%i: Master starting with %i workers'%(rank, num_workers)

    set_number = 0
    while closed_workers < num_workers:
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()

        if tag == tags.EXIT:
            # Worker received exit tag and exited
            print '%i: Worker %i exited.'%(rank, source)
            closed_workers += 1

        elif tag == tags.DONE:
            # Worker finished task and returned result
            print '%i: Master received DONE Signal from worker %i'%(rank, source)

            # Execute master's function
            all_results.append(function(data, *args))
            
            if task_index < len(tasks):
                # still tasks to be sent to worker
                task = tasks[task_index]
                comm.send(task, dest=source, tag=tags.START)
                print '%i: Sending task %i to worker %i'%(rank, task_index, source)
                task_index += 1
            else:
                # all tasks already complete/sent
                # tell worker to exit
                comm.send(None, dest=source, tag=tags.EXIT)
                print '%i: No more jobs, sending Exit Signal to worker %i'%(rank, source)

        elif tag == tags.READY and task_index < len(tasks):
            # Worker is ready to receive task, and more tasks are available 
            task = tasks[task_index]
            task_index += 1
            print '%i: Received ready signal from worker %i'%(rank, source)
            comm.send(task, dest=source, tag=tags.START)
            print '%i: Sending task %i to worker %i'%(rank, task_index, source)

        else:
            # No jobs
            # Tell worker to exit
            comm.send(None, dest=source, tag=tags.EXIT)
            print '%i: No more jobs, sending Exit Signal to worker %i'%(rank, source)
        
        print '%i: %i tasks left'%(rank, len(tasks) - task_index)
        print '%i: %i workers still busy'%(rank, num_workers - closed_workers)

    print '%i: Master finished sending and receiving'%rank
    return all_results      # List of results from all the "master" function on the workers results. 

def worker_task_receiver(function):
    '''
    Worker processors execute code below
    `function`      A function that accepts a single task from the master 
                    and is the excecuted by the worker. 
    '''
    print '%i: Worker on %s'%(rank, name)
    comm.send(None, dest=0, tag=tags.READY)

    while True:
        task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        if tag == tags.START:
            # Do the work here
            print '%i: Worker received'%rank, task, 'to do.'
            # Main is "THE" function per task
            result = function(task)
            comm.send(result, dest=0, tag=tags.DONE)
        elif tag == tags.EXIT:
            break

    print '%i: Worker exiting.'%rank
    comm.send(None, dest=0, tag=tags.EXIT)


# End
#------------------------------------------------------------------------------

