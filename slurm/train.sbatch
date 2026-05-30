#!/usr/bin/env bash
# PLACEHOLDER (Phase 3+). GPU training of the Latent Atlas surrogate/retrieval
# model. Do NOT submit until a dataset exists (run phenotype_gen + merge first)
# and the training entry point is written. Models are small — one GPU suffices.
#
# Runs:  ml/training/train.py  (PENDING — Phase 3/4 deliverable)

#SBATCH --job-name=bne-train
#SBATCH --partition=__GPU_PARTITION__      # TODO: gpu partition
#SBATCH --account=__ACCOUNT__              # TODO or delete
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=06:00:00
#SBATCH --output=logs/train_%j.out
#SBATCH --error=logs/train_%j.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
# module load cuda/12 python/3.11          # TODO: adjust to your cluster

cd "$BNC_ROOT"
echo "PLACEHOLDER: wire to ml/training/train.py once Phase-2 dataset + model exist."
exit 1
