const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')

fs.mkdirSync('input/dht-addrs', { recursive: true })

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/miner-report-dht-miner-peer-scanner',
    ['minerDhtAddrs', 'currentEpochDate']
    // { headless: false }
  )
  const currentEpoch = await notebook.value('currentEpoch')
  const currentEpochDate = await notebook.value('currentEpochDate')
  console.log('Date:', currentEpochDate)
  let count = 0
  while (true) {
    const dhtAddrs = await notebook.value('minerDhtAddrs')
    if (dhtAddrs.state === 'paused') {
      await notebook.redefine('start', 1)
      await delay(1000)
      continue
    }
    if (count++ % 100 === 0) {
      console.log(
        'DHT Miners => State:',
        dhtAddrs.state,
        dhtAddrs.elapsed ? `Elapsed: ${dateFns.formatDistance(dhtAddrs.elapsed * 1000, 0)}` : '',
        'Scanned:',
        dhtAddrs.scannedPeers + '/' + dhtAddrs.totalPeers,
        'Records:',
        dhtAddrs.recordsLength,
        'Errors:',
        dhtAddrs.errors
      )
    }
    if (dhtAddrs.state === 'done') {
      jsonFilename = `dht-addrs-${currentEpoch}.json`
      const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
      for (const record of dhtAddrs.records) {
        await jsonFile.write(
          JSON.stringify({
            timestamp: currentEpochDate,
            epoch: currentEpoch,
            collectedFrom: 'jim-ovh-1',
            ...record
          }) + '\n'
        )
      }
      jsonFile.on('finish', () => {
        fs.rename(`tmp/${jsonFilename}`, `input/dht-addrs/${jsonFilename}`, err => {
          if (err) {
            console.error('Error', err)
            process.exit(1)
          }
        })
      })
      jsonFile.end()
      break
    }
  }
  console.log('Filename:', jsonFilename)
  console.log('Epoch:', currentEpoch)
  console.log('Date:', currentEpochDate)
  await notebook.browser.close()
}
run()
