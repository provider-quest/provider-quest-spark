const fs = require('fs')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')
require('dotenv').config()

const workDir = process.env.WORK_DIR || '.'
const tmpDir = `${workDir}/tmp`

fs.mkdirSync(`${workDir}/input/ips-baidu`, { recursive: true })

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/miner-report-baidu-ip-geo-lookups',
    ['ipsBaidu', 'currentEpoch', 'currentEpochDate']
    // { headless: false }
  )
  const currentEpoch = await notebook.value('currentEpoch')
  const currentEpochDate = await notebook.value('currentEpochDate')
  console.log('Date:', currentEpochDate)
  let count = 0
  while (true) {
    const ipsBaidu = await notebook.value('ipsBaidu')
    if (ipsBaidu.state === 'paused') {
      await notebook.redefine('geoIpBaiduKey', process.env.GEOIP_BAIDU_KEY.trim())
      await notebook.redefine('geoIpBaiduSecret', process.env.GEOIP_BAIDU_SECRET.trim())
      await notebook.redefine('maxLookups', 50)
      await notebook.redefine('maxElapsed', 1 * 60 * 1000)
      await notebook.redefine('start', 1)
      await delay(1000)
      continue
    }
    if (count++ % 100 === 0) {
      console.log(
        `ips-baidu${process.argv[2] ? ' ' + process.argv[2] : ''} => State:`,
        ipsBaidu.state,
        ipsBaidu.elapsed ? `Elapsed: ${dateFns.formatDistance(ipsBaidu.elapsed * 1000, 0)}` : '',
        'Scanned:',
        ipsBaidu.scannedIps + '/' + ipsBaidu.totalIps,
        'Records:',
        ipsBaidu.recordsLength,
        'Errors:',
        ipsBaidu.errors
      )
    }
    if (ipsBaidu.state === 'done') {
      console.log('Last error:', ipsBaidu.lastError)
      count = ipsBaidu.records.length
      jsonFilename = `ips-baidu-${currentEpoch}.json`
      const jsonFile = fs.createWriteStream(`${tmpDir}/${jsonFilename}`)
      for (const record of ipsBaidu.records) {
        await jsonFile.write(
          JSON.stringify({
            timestamp: currentEpochDate,
            epoch: currentEpoch,
            ...record
          }) + '\n'
        )
      }
      jsonFile.on('finish', () => {
        fs.rename(`${tmpDir}/${jsonFilename}`, `${workDir}/input/ips-baidu/${jsonFilename}`, err => {
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
