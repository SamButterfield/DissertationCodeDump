#!/bin/bash
#SBATCH --account=pmp4nu
#SBATCH --job-name=CPH_sample_extraction
#SBATCH --mail-type=ALL
#SBATCH --mail-user=s.butterfield@ncl.ac.uk
#SBATCH --chdir=/mnt/nfs/home/b6029496/sam_butterfield_diss_code
#SBATCH --cpus-per-task=32
#SBATCH -t 01:00:00
#SBATCH --error=slurm-%j.err
#SBATCH --output=slurm-%j.out
module load Python
python 1_population_extraction.py