#!/bin/sh
rabix --job 'rabix/tests/test_runtime/bwa-mem.yml#job'
rabix --tool 'rabix/tests/test_runtime/bwa-mem-tool.yml#tool' -- --reads rabix/tests/test-files/example_human_Illumina.pe_1.fastq --reads rabix/tests/test-files/example_human_Illumina.pe_2.fastq --reference rabix/tests/test-files/example_human_reference.fasta