#!/bin/bash

set -e

export BENCHMARK_RUN='BenchmarkOTLP|BenchmarkAgentAll'
export BENCHMARK_AGENTS=512
export BENCHMARK_WARMUP_TIME=1m

SHARDS=( 1 5 10 15 20 24 )

for shard in "${SHARDS[@]}"; do
	export TF_VAR_apm_shards=${shard}
    make all cleanup-elasticsearch run-benchmark
    grep '^Benchmark' benchmark-result.txt > aws-qa-8g-otlp-${shard}s-${BENCHMARK_AGENTS}a.txt
done
