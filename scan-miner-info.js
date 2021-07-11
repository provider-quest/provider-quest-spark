const dns = require('dns')
const fs = require('fs')
const util = require('util')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')

const dnsLookup = util.promisify(dns.lookup)

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/miner-report-miner-info-scanner',
    ['minerInfo', 'selectedDate']
    // { headless: false }
  )
  const selectedEpoch = await notebook.value('selectedEpoch')
  const selectedDate = await notebook.value('selectedDate')
  console.log('Date:', selectedDate)
  const tipSet = await notebook.value('tipSet')
  let count = 0
  while (true) {
    const minerInfo = await notebook.value('minerInfo')
    if (count++ % 100 === 0) {
      console.log(
        'Miner Info => State: ',
        minerInfo.state,
        ' Elapsed: ',
        minerInfo.elapsed,
        ' Records: ',
        minerInfo.recordsLength
      )
    }
    if (minerInfo.state === 'done') {
      jsonFilename = `info-${selectedEpoch}.json`
      const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
      for (const record of minerInfo.records) {
        const { height, multiaddrsDecoded, ...rest } = record
        let dnsLookups
        if (multiaddrsDecoded) {
          for (const maddr of multiaddrsDecoded) {
            const match = maddr.match(/^\/dns[46]\/([^\/]+)/)
            if (match) {
              const dnsHost = match[1]
              console.log('Resolving', dnsHost)
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
            timestamp: selectedDate,
            epoch: selectedEpoch,
            tipSet,
            ...rest,
            multiaddrsDecoded,
            dnsLookups
          }) + '\n'
        )
      }
      jsonFile.on('finish', () => {
        fs.rename(`tmp/${jsonFilename}`, `input/miner-info/${jsonFilename}`, err => {
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
  console.log('Epoch:', selectedEpoch)
  console.log('Date:', selectedDate)
  console.log('TipSet:', tipSet)
  await notebook.browser.close()
}
run()
