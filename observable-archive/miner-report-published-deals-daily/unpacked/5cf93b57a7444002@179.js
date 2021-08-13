// https://observablehq.com/@jimpick/miner-report-feeds@179
import define1 from "./c4e4a355c53d2a1a@33.js";

export default function define(runtime, observer) {
  const main = runtime.module();
  main.variable(observer()).define(["md"], function(md){return(
md`# Miner.Report Feeds/Buckets
`
)});
  main.variable(observer()).define(["md","quickMenu"], function(md,quickMenu){return(
md`${quickMenu}`
)});
  main.variable(observer()).define(["md"], function(md){return(
md`
This is a list of feeds generated continuously by the Miner.Report system, distributed using [Textile Buckets](https://docs.textile.io/buckets/) and IPFS. Feel free to use this data in your own projects!`
)});
  main.variable(observer()).define(["md","legacyWorkshopClientBucketUrl"], function(md,legacyWorkshopClientBucketUrl){return(
md`## Legacy Workshop Client

This bucket contains annotations and other metadata from manual deal testing using the [workshop-client-mainnet](https://github.com/jimpick/workshop-client-mainnet) web-based client. Every few days I attempt small deals against all the miners I can find and I record the annotations in a JSON file. This will be gradually replaced with an ObservableHQ/Apache Spark solution.

* Textile Bucket: [legacy-workshop-client](${legacyWorkshopClientBucketUrl})`
)});
  main.variable(observer("legacyWorkshopClientBucketUrl")).define("legacyWorkshopClientBucketUrl", function(){return(
'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeiebnqxhltzfhmhdfr2j6p24ngipc7n4qmxrufivvjat7iwjazfwp4'
)});
  main.variable(observer("annotatedMinerIndexes")).define("annotatedMinerIndexes", ["legacyWorkshopClientBucketUrl"], async function(legacyWorkshopClientBucketUrl){return(
(await (await fetch(`${legacyWorkshopClientBucketUrl}/annotated-miner-indexes.json`)).json()).map(num => `f0${num}`)
)});
  main.variable(observer("annotatedMinerIndexesExcludingDelisted")).define("annotatedMinerIndexesExcludingDelisted", ["legacyWorkshopClientBucketUrl"], async function(legacyWorkshopClientBucketUrl){return(
(await (await fetch(`${legacyWorkshopClientBucketUrl}/annotated-miner-indexes-excluding-delisted.json`)).json()).map(num => `f0${num}`)
)});
  main.variable(observer("legacyAnnotationsMainnet")).define("legacyAnnotationsMainnet", ["legacyWorkshopClientBucketUrl"], async function(legacyWorkshopClientBucketUrl){return(
(await fetch(`${legacyWorkshopClientBucketUrl}/annotations-mainnet.json`)).json()
)});
  main.variable(observer()).define(["Inputs","legacyAnnotationsMainnet"], function(Inputs,legacyAnnotationsMainnet){return(
Inputs.table(Object.entries(legacyAnnotationsMainnet))
)});
  main.variable(observer()).define(["md","minerInfoSubsetLatestBucketUrl"], function(md,minerInfoSubsetLatestBucketUrl){return(
md`## Miner Info

Every couple of hours, I collect information about the miners I am tracking from the Lotus API using the [Miner Info Scanner notebook](https://observablehq.com/@jimpick/miner-report-miner-info-scanner). This on-chain data includes information such as PeerIDs and multiaddresses (IP addresses) useful for communicating peer-to-peer with miners.

* Textile Bucket: [miner-info-subset-latest](${minerInfoSubsetLatestBucketUrl}) (Latest miner info only)
`
)});
  main.variable(observer("minerInfoSubsetLatestBucketUrl")).define("minerInfoSubsetLatestBucketUrl", function(){return(
'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeigscdljg5a32b4toh6jkz4h3dczusqd6s3mkt3h5nbwtqmbqmh6mu'
)});
  main.variable(observer("minerInfoSubsetLatest")).define("minerInfoSubsetLatest", ["minerInfoSubsetLatestBucketUrl"], async function(minerInfoSubsetLatestBucketUrl){return(
(await fetch(`${minerInfoSubsetLatestBucketUrl}/miner-info-subset-latest.json`)).json()
)});
  main.variable(observer()).define(["Inputs","minerInfoSubsetLatest"], function(Inputs,minerInfoSubsetLatest){return(
Inputs.table(Object.entries(minerInfoSubsetLatest.miners).map(([miner, info]) => ({miner, ...info})))
)});
  main.variable(observer()).define(["md","asksSubsetLatestBucketUrl"], function(md,asksSubsetLatestBucketUrl){return(
md`## Asks

* Textile Bucket: [asks-subset-latest](${asksSubsetLatestBucketUrl})
`
)});
  main.variable(observer("asksSubsetLatestBucketUrl")).define("asksSubsetLatestBucketUrl", function(){return(
'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeidg5ygzrk4oxusmopijf7wqxibmm3sktkkhuz7sfecextyuvifx7y'
)});
  main.variable(observer("asksSubsetLatest")).define("asksSubsetLatest", ["asksSubsetLatestBucketUrl"], async function(asksSubsetLatestBucketUrl){return(
(await fetch(`${asksSubsetLatestBucketUrl}/asks-subset-latest.json`)).json()
)});
  main.variable(observer()).define(["Inputs","asksSubsetLatest"], function(Inputs,asksSubsetLatest){return(
Inputs.table(Object.entries(asksSubsetLatest.miners).map(([miner, ask]) => ({miner, ...ask})))
)});
  main.variable(observer()).define(["md","dealsBucketUrl"], function(md,dealsBucketUrl){return(
md`## Deals

* Textile Bucket: [deals](${dealsBucketUrl})
`
)});
  main.variable(observer("dealsBucketUrl")).define("dealsBucketUrl", function(){return(
'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeidhnns26omq6a3y4jdixo7nqvb27wn7otfowohei5zibupvh7d2hq/'
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Miner Power`
)});
  main.variable(observer()).define(["md","minerPowerDailyAverageLatestBucketUrl","minerPowerMultidayAverageLatestBucketUrl"], function(md,minerPowerDailyAverageLatestBucketUrl,minerPowerMultidayAverageLatestBucketUrl){return(
md`
* [miner-power-daily-average-latest](${minerPowerDailyAverageLatestBucketUrl})
* [miner-power-multiday-average-latest](${minerPowerMultidayAverageLatestBucketUrl})
`
)});
  main.variable(observer("minerPowerDailyAverageLatestBucketUrl")).define("minerPowerDailyAverageLatestBucketUrl", function(){return(
"https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeiehszmgeygov7bqchfmhh5zxtmn6xyt26ufyhw5k6tuy23h2w4ngm"
)});
  main.variable(observer("minerPowerMultidayAverageLatestBucketUrl")).define("minerPowerMultidayAverageLatestBucketUrl", function(){return(
'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeicdqsds5fkmmcrtkyg3uf6zk5t3bralisswrdh5wlo25przr23pqq'
)});
  main.variable(observer()).define(["md","dhtAddrsLatestBucketUrl"], function(md,dhtAddrsLatestBucketUrl){return(
md`## DHT Addresses

On a regular basis, peer lookups are made against the DHT (Distributed Hash Table) using the [@jimpick/miner-report-dht-miner-peer-scanner](https://observablehq.com/@jimpick/miner-report-dht-miner-peer-scanner?collection=@jimpick/miner-report) notebook and published here.

* Textile Bucket: [dht-addrs-latest](${dhtAddrsLatestBucketUrl})`
)});
  main.variable(observer("dhtAddrsLatestBucketUrl")).define("dhtAddrsLatestBucketUrl", function(){return(
"https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeifpxwjdz5e5mv3tzat7de6uzxfusfqk5flqfrlh7re3ria6bs7ye4/"
)});
  main.variable(observer()).define(["md","multiaddrsIpsLatestBucketUrl"], function(md,multiaddrsIpsLatestBucketUrl){return(
md`## Multiaddresses and IP Addresses

The scan data from the "Miner Info" and "DHT Addresses" scans are combined using the [@jimpick/miner-report-multiaddr-ip-tool](https://observablehq.com/@jimpick/miner-report-multiaddr-ip-tool?collection=@jimpick/miner-report) notebook and published here.

* Textile Bucket: [multiaddrs-ips-latest](${multiaddrsIpsLatestBucketUrl})`
)});
  main.variable(observer("multiaddrsIpsLatestBucketUrl")).define("multiaddrsIpsLatestBucketUrl", function(){return(
'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeia7ab6fddp255zwn3i4r36pp5walbnblkojfhbuesvsxywmvbk3sa'
)});
  main.variable(observer()).define(["md","geoIpLookupsBucketUrl"], function(md,geoIpLookupsBucketUrl){return(
md`## GeoIP Lookups

The list of IPs is cross-references with databases to lookup geographic locations. The [MaxMind GeoLite2 scanner notebook](https://observablehq.com/@jimpick/miner-report-maxmind-geolite2-lookups?collection=@jimpick/miner-report) is used to perform lookups against a freely downloadable database.

* Textile Bucket: [geoip-lookups](${geoIpLookupsBucketUrl})`
)});
  main.variable(observer("geoIpLookupsBucketUrl")).define("geoIpLookupsBucketUrl", function(){return(
'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeibjg7kky45npdwnogui5ffla7dint62xpttvvlzrsbewlrfmbusya'
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Imports`
)});
  const child1 = runtime.module(define1);
  main.import("quickMenu", child1);
  return main;
}
