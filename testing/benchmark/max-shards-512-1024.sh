#!/bin/bash

export BENCHMARK_RUN='BenchmarkOTLP|BenchmarkAgentAll'
export BENCHMARK_WARMUP_TIME=5m
export TF_VAR_apm_shards=24

make apply cleanup-elasticsearch run-benchmark BENCHMARK_AGENTS=512
grep '^Benchmark' benchmark-result.txt > aws-qa-15g-400sem-500q-max-20-active-30-flush-discard-512a.txt

make cleanup-elasticsearch run-benchmark BENCHMARK_AGENTS=1024
grep '^Benchmark' benchmark-result.txt > aws-qa-15g-400sem-500q-max-20-active-30-flush-discard-1024a.txt
