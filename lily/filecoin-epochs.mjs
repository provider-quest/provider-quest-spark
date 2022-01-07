// I thought I saw an npm package somewhere but I can't find it.

// From: https://observablehq.com/@jbenet/filecoin-chain-time-calculator (no license)

// Also: https://observablehq.com/@protocol/filecoin-epoch-calculator (no license)

const filecoinNetworkStartTime = new Date("2020-08-24T22:00:00Z")

const blockTimeSec = 30

export function epochToDate(epoch) {
  var diff_s = blockTimeSec * epoch
  var diff_ms = diff_s * 1000
  var date = new Date(filecoinNetworkStartTime.getTime() + diff_ms)
  return date
}

export function dateToEpoch(date) {
  var diff_ms = (date - filecoinNetworkStartTime)
  if (diff_ms < 0) {
    throw new Error("date must be greater than filecoinNetworkStartTime")
  }
  
  var diff_s = Math.floor(Math.abs(diff_ms) / 1000)
  var epoch = Math.floor(diff_s / blockTimeSec)
  return epoch
}


