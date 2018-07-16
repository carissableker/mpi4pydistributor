#! /usr/bin/env python

import numpy as np
import mpi4py_distributor as mpi
import subprocess
import argparse
import glob
import os
from Bio import SeqIO

def arguments():
    parser = argparse.ArgumentParser(description='Given a file containing a list of fasta files, Run all-against-all BLASTp.')
    parser.add_argument("input_file", type=str, help="Input file with one fasta file per line. (Including full path.)")
    parser.add_argument("output_folder", type=str, help="Folder to store results and intermediate files")
    parser.add_argument("--eValue", type=float, help="e-value cutoff for BLASTp", default=10e-6)
    args = parser.parse_args()

    return args

def combinations(n):
    D = {x:[] for x in range(n)}
    for i in range(n):
        for j in range(i+1):
            if (i+j)%2 == 0:
                D[i].append(j)

            else:
                D[j].append(i)
    # turn it into a list of tuples
    L = []
    for i in D:
        L.append((i, D[i]))

    return L

def sequence_length(i):
    fasta_file_i = list_of_fasta_files[i]

    l = 0
    with open(fasta_file_i, 'r') as handle:
        for record in SeqIO.parse(handle, "fasta"):
            l += len(record.seq)
    return l

def delete_blastdb(dbname):

    # list all relevant files
    dbfiles = glob.glob(dbname+'*')
    for f in dbfiles:
        os.remove(f)
    return 0

def round_blast((i, all_js)):

    fasta_file_i = list_of_fasta_files[i]
    ID_i =  os.path.splitext(os.path.split(fasta_file_i)[1])[0] 
    
    # make folder 
    results_folder = os.path.join(args.output_folder, ID_i)
    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    # create db i
    dbname = os.path.join(db_folder,  ID_i + '_blastdb')
    command = ['makeblastdb', '-in', fasta_file_i, '-dbtype', 'prot', '-out', dbname]
    subprocess.call(command)

    # do blast
    for j in all_js:
        fasta_file_j = list_of_fasta_files[j]
        ID_j =  os.path.splitext(os.path.split(fasta_file_j)[1])[0] 
        results_file = os.path.join(results_folder,  ID_i + '_' + ID_j + '_blastp_results.tsv')
        if not os.path.isfile(results_file):
            # blast i vs j
            command = ['blastp', '-query', fasta_file_j, '-db', dbname, '-dbsize', str(dbsize), '-evalue', args.eValue, '-outfmt',  '6 qseqid sseqid qlen slen pident evalue bitscore', '-out', results_file] 
            subprocess.call(command)

    # delete db
    delete_blastdb(dbname)

#########################################
## Main script
########################################

rank = mpi.rank
args = arguments()

db_folder = os.path.join(args.output_folder, 'blast_db')

if mpi.rank == 0:
    # make folder for dbs
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)

    # read in file for input
    list_of_fasta_files = []
    with open(args.input_file, 'r') as handle:
        for line in handle:
            list_of_fasta_files.append(os.path.abspath(line.strip()))

else: 
    list_of_fasta_files = None

# scatter file names to all workers
list_of_fasta_files = mpi.comm.bcast(list_of_fasta_files, root=0)

# calculate effective dbsize
# total length of all seqences across all files
if mpi.rank == 0:
    n = len(list_of_fasta_files)
    all_lengths = mpi.master_task_distributer(range(n)) 
    dbsize = sum(all_lengths)
       
else:
    mpi.worker_task_receiver(sequence_length)
    dbsize = None

# scatter dbsize to all workers
dbsize = mpi.comm.bcast(dbsize, root=0)

# now, blastp
if mpi.rank == 0:
    L = combinations(n)
    mpi.master_task_distributer(L)

else:
    mpi.worker_task_receiver(round_blast)





