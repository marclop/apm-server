# Scaling APM Server over 8GB

This folder contains the results of running the `apmbench` benchmarks against an Elastic Cloud
deployment 8GB or higher. The idea is to improve resource utilization on the APM Server side,
since preliminary data indicates that the current internal architecture doesn't automatically
scale-up as well as we would have liked. Initial benchmarks are available in `scaling/initial`.

The main issue on the APM Server is that CPU utilization remains ~30-45% with the default settings
on an 8GB instance with the `BenchmarkAgentAll` and `BenchmarkOTLP` benchmarks.

| APM Server size | ES instance size | ES instances | ES dedicated masters | ES zones | Hardware      |
| --------------- | -----------------| ------------ | -------------------- | -------- |-------------- |
| 8 GB            | 58GB             | 6            | 3                    | 3        | CPU Optimized |
| 15 GB           | 58GB             | 6            | 3                    | 3        | CPU Optimized |

The folder structure follows the following nomenclature:

    scaling/<apm-server-size>/flushbytes/<#>mb/<#indexers>/<csp>-<env>-<apm-server-size>-<shards>s-<agents>-a.txt

As an example, an APM Server benchmarked with 3MB as the maximum bulk indexer cache, with 10 available
bulk indexers, Elasticsearch indices configured with 15 shards each, and running the benchmarks with
512 agents:

    scaling/8g/flushbytes/3mb/10-default/aws-qa-8g-otlp-15s-512a.txt

To compare the benchmarks results, it's best to use `benchstat` and use a high `-alpha` with `-geomean` set.

The benchmarks have been run using `../bench.sh`.
