#!/bin/bash
#SBATCH -p compute2
#SBATCH --nodes=10
#SBATCH -A k20200
#SBATCH -d singleton
#SBATCH -m cyclic
#SBATCH -J md-real-io

srun --ntasks-per-node=24 ./md-real-io -T=/home/dkrz/k202079/work/md-real-io/test -O=1 -D=10 -P=3000 -N=1000 -F=3900 --thread-reports


