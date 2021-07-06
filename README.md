miner-report-spark
---

Data pipeline for Filecoin analytical data for https://miner.report/

The following scripts live here:

* scripts to collect data from "Miner.Report" ObservableHQ notebooks and
  store the data as JSON files
* a Pyspark script to run Apache Spark Structured Streaming, which ingests
  the JSON files continuously, and generates new JSON files with
  aggregations and mapping
* scripts to publish aggregrated data to Textile Buckets for syndication

## Documentation

* https://observablehq.com/@jimpick/miner-report-documentation?collection=@jimpick/miner-report

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/lotus/blob/master/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/lotus/blob/master/LICENSE-APACHE)

Data is licenced as [CC-BY-SA 3.0](https://ipfs.io/ipfs/QmVreNvKsQmQZ83T86cWSjPu2vR3yZHGPm5jnxFuunEB9u) unless otherwised noted.
