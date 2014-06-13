#!/bin/sh
python -m rabix.runtime.cli run rabix/tests/apps/pipeline_test_bwa_freebayes.json \
  --reference rabix/tests/test-files/example_human_reference.fasta \
  --read rabix/tests/test-files/example_human_Illumina.pe_1.fastq \
  --read rabix/tests/test-files/example_human_Illumina.pe_2.fastq
