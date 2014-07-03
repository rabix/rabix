#!/bin/sh
python -m rabix.cli run -v \
  rabix/tests/apps/pipeline_test_bwa_freebayes.json \
  --reference https://s3.amazonaws.com/boysha/testfiles/example_human_reference.fasta \
  --read https://s3.amazonaws.com/boysha/testfiles/example_human_Illumina.pe_1.fastq \
  --read https://s3.amazonaws.com/boysha/testfiles/example_human_Illumina.pe_2.fastq
