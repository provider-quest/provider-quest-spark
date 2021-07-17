const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')

fs.mkdirSync('input/multiaddrs-ips', { recursive: true })

async function run () {
  const notebook = await load(
    '@jimpick/miner-report-multiaddr-ip-tool',
    ['minerMultiaddrIps']
    // { headless: false }
  )
  const minerMultiaddrIps = await notebook.value('minerMultiaddrIps')
  await notebook.browser.close()
  let lastEpoch = 0
  for (const record of minerMultiaddrIps) {
    if (record.epoch > lastEpoch) lastEpoch = record.epoch
  }
  const jsonFilename = `multiaddrs-ips-${lastEpoch}.json`
  const dest =`input/multiaddrs-ips/${jsonFilename}` 
  if (fs.existsSync(dest)) {
    console.log(`File already exists, skipping. ${jsonFilename}`)
  } else {
    const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
    for (const record of minerMultiaddrIps) {
      await jsonFile.write(JSON.stringify(record) + '\n')
    }
    jsonFile.on('finish', () => {
      fs.rename(`tmp/${jsonFilename}`, dest, err => {
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
