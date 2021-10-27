import fs from 'fs'
import fetch from 'node-fetch'

const geoIpLookupsBucketUrl = 'https://hub.textile.io/thread/bafkwblbznyqkmqx5l677z3kjsslhxo2vbbqh6wluunvvdbmqattrdya/buckets/bafzbeibjg7kky45npdwnogui5ffla7dint62xpttvvlzrsbewlrfmbusya'

const locationsReport = await ((await fetch(`${geoIpLookupsBucketUrl}/miner-locations-latest.json`)).json())

const providerLocations = {}

for (const record of locationsReport.minerLocations) {
  const provider = record.miner
  if (!providerLocations[provider]) {
    providerLocations[provider] = []
  }
  providerLocations[provider].push([record.long, record.lat])
}

const providerEvents = []

for (const provider in providerLocations) {
  const eventsFile = `../dist/provider-power-level-events/${provider}.json`
  if (fs.existsSync(eventsFile)) {
    const contents = fs.readFileSync(eventsFile, 'utf8')
    const events = JSON.parse(contents)
    const providerEvent = {
      provider,
      locations: providerLocations[provider],
      events: events.map(({ date, level }) => ({ date, level }))
    }
    providerEvents.push(providerEvent)
  }
}
// console.log(JSON.stringify(providerEvents, null, 2))
console.log(JSON.stringify(providerEvents))
