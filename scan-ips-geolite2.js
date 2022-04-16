const fs = require('fs')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')

const workDir = process.env.WORK_DIR || '.'
const tmpDir = `${workDir}/tmp`

fs.mkdirSync(`${workDir}/ips-geolite2`, { recursive: true })

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/miner-report-maxmind-geolite2-lookups',
    ['ipsGeoLite2', 'currentEpoch', 'currentEpochDate']
    // { headless: false }
  )
  const currentEpoch = await notebook.value('currentEpoch')
  const currentEpochDate = await notebook.value('currentEpochDate')
  console.log('Date:', currentEpochDate)
  let count = 0
  while (true) {
    const ipsGeoLite2 = await notebook.value('ipsGeoLite2')
    if (ipsGeoLite2.state === 'paused') {
      /*
      if (process.argv[2] === '--fail-only') {
        await notebook.redefine('maxElapsed', 5 * 60 * 1000)
        await notebook.redefine('subsetToScan', 'Fail only')
      } else if (process.argv[2] === '--no-recents') {
        await notebook.redefine('maxElapsed', 5 * 60 * 1000)
        await notebook.redefine('subsetToScan', 'No recents')
      } else {
      */
      await notebook.redefine('maxElapsed', 3 * 60 * 1000)
      await notebook.redefine('start', 1)
      await delay(1000)
      continue
    }
    if (count++ % 100 === 0) {
      console.log(
        `ips-geolite2${process.argv[2] ? ' ' + process.argv[2] : ''} => State:`,
        ipsGeoLite2.state,
        ipsGeoLite2.elapsed ? `Elapsed: ${dateFns.formatDistance(ipsGeoLite2.elapsed * 1000, 0)}` : '',
        'Scanned:',
        ipsGeoLite2.scannedIps + '/' + ipsGeoLite2.totalIps,
        'Records:',
        ipsGeoLite2.recordsLength,
        'Errors:',
        ipsGeoLite2.errors
      )
    }
    if (ipsGeoLite2.state === 'done') {
      count = ipsGeoLite2.records.length
      jsonFilename = `ips-geolite2-${currentEpoch}.json`
      const jsonFile = fs.createWriteStream(`${tmpDir}/${jsonFilename}`)
      for (const record of ipsGeoLite2.records) {
        await jsonFile.write(
          JSON.stringify({
            timestamp: currentEpochDate,
            epoch: currentEpoch,
            ...record
          }) + '\n'
        )
      }
      jsonFile.on('finish', () => {
        fs.rename(`${tmpDir}/${jsonFilename}`, `${workDir}/ips-geolite2/${jsonFilename}`, err => {
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
  console.log('Records:', count)
  await notebook.browser.close()
}
run()
