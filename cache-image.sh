#!/bin/bash

set -u

export TMPDIR=/ssd/home/"$USER"/TEMP

IMAGE=$1
sbatch \
    --wrap="srun --output=cache-image-%N-%j.out podman pull --authfile /mnt/apps/etc/auth.json $IMAGE" \
    --nodes=4 \
    --output=/dev/null # Output is captured by srun
