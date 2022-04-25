const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')

const workDir = process.env.WORK_DIR || '.'
const tmpDir = `${workDir}/tmp`

const outputDir = process.env.MULTIADDRS_IPS_DIR || `${workDir}/input/multiaddrs-ips`
fs.mkdirSync(outputDir, { recursive: true })

async function run () {
  const notebook = await load(
    '@jimpick/miner-report-multiaddr-ip-tool',
    ['minerMultiaddrIps', 'deltaMultiaddrsIps']
    // { headless: false }
  )
  // const minerMultiaddrIps = await notebook.value('minerMultiaddrIps')
  const minerMultiaddrIps = await notebook.value('deltaMultiaddrsIps')
  await notebook.browser.close()
  let lastEpoch = 0
  for (const record of minerMultiaddrIps) {
    if (record.epoch > lastEpoch) lastEpoch = record.epoch
  }
  const jsonFilename = `multiaddrs-ips-${lastEpoch}.json`
  const dest =`${outputDir}/${jsonFilename}` 
  if (fs.existsSync(dest)) {
    console.log(`File already exists, skipping. ${jsonFilename}`)
  } else {
    const jsonFile = fs.createWriteStream(`${workDir}/tmp/${jsonFilename}`)
    for (const record of minerMultiaddrIps) {
      await jsonFile.write(JSON.stringify(record) + '\n')
    }
    jsonFile.on('finish', () => {
      fs.rename(`${workDir}/tmp/${jsonFilename}`, dest, err => {
        if (err) {
          console.error('Error', err)
          process.exit(1)
        }
      })
    })
    jsonFile.end()
    console.log('Filename:', jsonFilename)
    console.log('Epoch:', lastEpoch)
    console.log('Records:', minerMultiaddrIps.length)
  }
  await notebook.browser.close()
}
run()
