// https://observablehq.com/@jimpick/miner-report-utils@33
export default function define(runtime, observer) {
  const main = runtime.module();
  main.variable(observer()).define(["md"], function(md){return(
md`# Miner.Report Utils`
)});
  main.variable(observer()).define(["md"], function(md){return(
md`Functions for re-use across various Miner.Report notebooks.`
)});
  main.variable(observer()).define(["sortMiners"], function(sortMiners){return(
['f01234', 'f013456', 'f0101234', 'f02345'].sort(sortMiners)
)});
  main.variable(observer("sortMiners")).define("sortMiners", function(){return(
(a, b) => Number(a.slice(1)) - Number(b.slice(1))
)});
  main.variable(observer()).define(["sortMinerRecords"], function(sortMinerRecords){return(
[{ miner: 'f01234' }, { miner: 'f013456' }, { miner: 'f0101234' }, { miner: 'f02345' }].sort(sortMinerRecords)
)});
  main.variable(observer("sortMinerRecords")).define("sortMinerRecords", function(){return(
({ miner: minerA }, { miner: minerB }) => Number(minerA.slice(1)) - Number(minerB.slice(1))
)});
  main.variable(observer("quickMenu")).define("quickMenu", function(){return(
`[Miner.Report](https://observablehq.com/collection/@jimpick/miner-report)
=> Deals: [Daily](https://observablehq.com/@jimpick/miner-report-published-deals-daily?collection=@jimpick/miner-report)
· [Hourly](https://observablehq.com/@jimpick/miner-report-published-deals-hourly?collection=@jimpick/miner-report)
· [Named Clients](https://observablehq.com/@jimpick/miner-report-deals-named-clients-hourly?collection=@jimpick/miner-report)
| [Asks](https://observablehq.com/@jimpick/miner-report-piece-size-vs-asks?collection=@jimpick/miner-report)
| [Map](https://observablehq.com/@jimpick/miner-report-miners-on-a-global-map?collection=@jimpick/miner-report)
· [Regional Stats](https://observablehq.com/@jimpick/miner-report-regional-stats?collection=@jimpick/miner-report)
| [Documentation](https://observablehq.com/@jimpick/miner-report-documentation?collection=@jimpick/miner-report)
| [Feeds](https://observablehq.com/@jimpick/miner-report-feeds?collection=@jimpick/miner-report)
`
)});
  main.variable(observer()).define(["md","quickMenu"], function(md,quickMenu){return(
md`${quickMenu}`
)});
  return main;
}
