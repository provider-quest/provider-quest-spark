const dns = require('dns')
const fs = require('fs')
const util = require('util')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')

const workDir = process.env.WORK_DIR || '.'
const tmpDir = `${workDir}/tmp`

fs.mkdirSync(`${workDir}/input/dht-addrs`, { recursive: true })

const dnsLookup = util.promisify(dns.lookup)

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
      if (process.argv[2] === '--fail-only') {
        await notebook.redefine('maxElapsed', 5 * 60 * 1000)
        await notebook.redefine('subsetToScan', 'Fail only')
      } else if (process.argv[2] === '--no-recents') {
        await notebook.redefine('maxElapsed', 5 * 60 * 1000)
        await notebook.redefine('subsetToScan', 'No recents')
      } else {
        await notebook.redefine('maxElapsed', 10 * 60 * 1000)
      }
      await delay(1000)
      continue
    }
    if (count++ % 100 === 0) {
      console.log(
        `dht-addrs${process.argv[2] ? ' ' + process.argv[2] : ''} => State:`,
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
      const jsonFile = fs.createWriteStream(`${tmpDir}/${jsonFilename}`)
      for (const record of dhtAddrs.records) {
        const { multiaddrs } = record
        let dnsLookups
        if (multiaddrs) {
          for (const maddr of multiaddrs) {
            const match = maddr.match(/^\/dns[46]\/([^\/]+)/)
            if (match) {
              const dnsHost = match[1]
              console.log('DNS Lookup', dnsHost)
              dnsLookups ||= {}
              dnsLookups[dnsHost] = await dnsLookup(
                dnsHost,
                { all: true, verbatim: true }
              )
            }
          }
        }
        await jsonFile.write(
          JSON.stringify({
            timestamp: currentEpochDate,
            epoch: currentEpoch,
            collectedFrom: 'jim-ovh-1',
            ...record,
            dnsLookups
          }) + '\n'
        )
      }
      jsonFile.on('finish', () => {
        fs.rename(`${tmpDir}/${jsonFilename}`, `${workDir}/input/dht-addrs/${jsonFilename}`, err => {
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
