#!/bin/bash

export BENCHMARK_RUN='BenchmarkOTLP|BenchmarkAgentAll'
export BENCHMARK_WARMUP_TIME=1m
export TF_VAR_apm_shards=24

make all cleanup-elasticsearch run-benchmark BENCHMARK_AGENTS=512
grep '^Benchmark' benchmark-result.txt > aws-qa-8g-nosem-24s-512a.txt

make all cleanup-elasticsearch run-benchmark BENCHMARK_AGENTS=1024
grep '^Benchmark' benchmark-result.txt > aws-qa-8g-nosem-24s-1024a.txt
