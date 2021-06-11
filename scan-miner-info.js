const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/lotus-js-client-mainnet-miner-info-scanner',
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
        'State: ',
        minerInfo.state,
        ' Elapsed: ',
        minerInfo.elapsed,
        ' Records: ',
        minerInfo.records && minerInfo.records.length
      )
    }
    if (minerInfo.state === 'done') {
      jsonFilename = `info-${selectedEpoch}.json`
      const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
      for (const record of minerInfo.records) {
        const { height, ...rest } = record
        await jsonFile.write(
          JSON.stringify({
            timestamp: selectedDate,
            epoch: selectedEpoch,
            tipSet,
            ...rest
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
