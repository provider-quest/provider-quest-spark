// https://observablehq.com/@jimpick/miner-report-published-deals-daily@347
import define1 from "./5cf93b57a7444002@179.js";
import define2 from "./c4e4a355c53d2a1a@33.js";

export default function define(runtime, observer) {
  const main = runtime.module();
  main.variable(observer()).define(["md"], function(md){return(
md`# Miner.Report Published Deals - Daily`
)});
  main.variable(observer()).define(["md","quickMenu"], function(md,quickMenu){return(
md`${quickMenu}`
)});
  main.variable(observer()).define(["md"], function(md){return(
md`Notes:

* Numbers are based on published deal proposals encoded in messages on the Filecoin blockchain.
* The [scanning script](https://observablehq.com/@jimpick/miner-report-publish-deal-messages-stream?collection=@jimpick/miner-report) is usually run multiple times per hour, but there may be gaps in the data from time to time. This system is optimized for timeliness instead of accuracy.
* Dates are based on when a message was published on the blockchain, not when the deal proposal was sent to a miner, or when the deal proving interval starts or when the sector is created.
* Not all published deals are successfully committed to sectors by miners, and occasionally miners lose data and get slashed, so these numbers will be greater than the actual amount of data stored to Filecoin.
* Distinct counts are calculated continuously with Spark Structured Streaming using an approximation algorithm.
* A similar report is available for [Hourly Deals](https://observablehq.com/@jimpick/miner-report-published-deals-hourly?collection=@jimpick/miner-report) and [Hourly Deals: Named Clients](https://observablehq.com/@jimpick/miner-report-deals-named-clients-hourly?collection=@jimpick/miner-report).
`
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Published Deals Per Day`
)});
  main.variable(observer()).define(["Plot","dailyDeals"], function(Plot,dailyDeals){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDeals, {x: "date", y: "count", fill: "#bab0ab"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Total Data Size of Published Deals Per Day (TiB)`
)});
  main.variable(observer()).define(["Plot","dailyDeals"], function(Plot,dailyDeals){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDeals, {x: "date", y: "sumPieceSizeTiB", fill: "#bab0ab"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Lifetime Value of Published Deals Per Day (FIL)`
)});
  main.variable(observer()).define(["Plot","dailyDeals"], function(Plot,dailyDeals){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDeals, {x: "date", y: "sum(lifetimeValue)", fill: "#bab0ab"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Number of Miners that Accepted Deals`
)});
  main.variable(observer()).define(["Plot","dailyDeals"], function(Plot,dailyDeals){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDeals, {x: "date", y: "approx_count_distinct(provider)", fill: "#bab0ab"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Number of Clients that Placed Deals`
)});
  main.variable(observer()).define(["Plot","dailyDeals"], function(Plot,dailyDeals){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDeals, {x: "date", y: "approx_count_distinct(client)", fill: "#bab0ab"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Number of Client <-> Miner Pairs`
)});
  main.variable(observer()).define(["Plot","dailyDeals"], function(Plot,dailyDeals){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDeals, {x: "date", y: "approx_count_distinct(clientProvider)", fill: "#bab0ab"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Distinct CIDs (Content Identifiers)`
)});
  main.variable(observer()).define(["Plot","dailyDeals"], function(Plot,dailyDeals){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDeals, {x: "date", y: "approx_count_distinct(label)", fill: "#bab0ab"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md `## Unverified/Verified: Deals Per Day`
)});
  main.variable(observer()).define(["Plot"], function(Plot){return(
Plot.plot({
  marks: [
    Plot.cell(['Unverified', 'Verified'], {x: d => d, fill: d => d})
  ]
})
)});
  main.variable(observer()).define(["Plot","dailyDealsByVerified"], function(Plot,dailyDealsByVerified){return(
Plot.plot({
  y: {
    grid: true
  },
  marks: [
    Plot.barY(dailyDealsByVerified, {x: "date", y: "count", fill: "verifiedDeal"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Unverified/Verified: Total Data Size (TiB)`
)});
  main.variable(observer()).define(["Plot"], function(Plot){return(
Plot.plot({
  marks: [
    Plot.cell(['Unverified', 'Verified'], {x: d => d, fill: d => d})
  ]
})
)});
  main.variable(observer()).define(["Plot","dailyDealsByVerified"], function(Plot,dailyDealsByVerified){return(
Plot.plot({
  y: {
    grid: true,
    label: `↑ TiB`
  },
  marks: [
    Plot.barY(dailyDealsByVerified, {x: "date", y: "sumPieceSizeTiB", fill: "verifiedDeal"}),
    Plot.ruleY([0])
  ]
})
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Data`
)});
  const child1 = runtime.module(define1);
  main.import("dealsBucketUrl", child1);
  main.variable(observer("dailyDealsRaw")).define("dailyDealsRaw", ["dealsBucketUrl"], async function(dealsBucketUrl){return(
(await fetch(`${dealsBucketUrl}/daily-totals.json`)).json()
)});
  main.variable(observer("dailyDeals")).define("dailyDeals", ["dailyDealsRaw","d3"], function(dailyDealsRaw,d3){return(
dailyDealsRaw.slice(-9).map(record => ({
  date: d3.isoParse(record.window.start),
  sumPieceSizeKiB: record['sum(pieceSizeDouble)'] / 1024,
  sumPieceSizeMiB: record['sum(pieceSizeDouble)'] / 1024 ** 2,
  sumPieceSizeGiB: record['sum(pieceSizeDouble)'] / 1024 ** 3,
  sumPieceSizeTiB: record['sum(pieceSizeDouble)'] / 1024 ** 4,
  ...record
}))
)});
  main.variable(observer()).define(["Inputs","dailyDeals"], function(Inputs,dailyDeals){return(
Inputs.table(dailyDeals.map(({window, ...rest}) => rest))
)});
  main.variable(observer("dailyDealsByVerifiedRaw")).define("dailyDealsByVerifiedRaw", ["dealsBucketUrl"], async function(dealsBucketUrl){return(
(await fetch(`${dealsBucketUrl}/daily-totals-verified.json`)).json()
)});
  main.variable(observer("dailyDealsByVerified")).define("dailyDealsByVerified", ["dailyDealsByVerifiedRaw","d3"], function(dailyDealsByVerifiedRaw,d3){return(
dailyDealsByVerifiedRaw.map(record => ({
  date: d3.isoParse(record.window.start),
  sumPieceSizeKiB: record['sum(pieceSizeDouble)'] / 1024,
  sumPieceSizeMiB: record['sum(pieceSizeDouble)'] / 1024 ** 2,
  sumPieceSizeGiB: record['sum(pieceSizeDouble)'] / 1024 ** 3,
  sumPieceSizeTiB: record['sum(pieceSizeDouble)'] / 1024 ** 4,
  ...record
})).sort((a, b) => {
  const comp1 = a.date - b.date
  if (comp1 !== 0) return comp1
  return Number(a.verifiedDeal) - Number(b.verifiedDeal)
}).slice(-18)
)});
  main.variable(observer()).define(["Inputs","dailyDealsByVerified"], function(Inputs,dailyDealsByVerified){return(
Inputs.table(dailyDealsByVerified.map(({window, ...rest}) => rest))
)});
  main.variable(observer()).define(["md"], function(md){return(
md`## Imports`
)});
  main.variable(observer("d3")).define("d3", ["require"], function(require){return(
require("d3@6")
)});
  const child2 = runtime.module(define2);
  main.import("quickMenu", child2);
  return main;
}
