const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/miner-report-storage-ask-scanner',
    ['asks', 'selectedDate']
    // { headless: false }
  )
  const selectedEpoch = await notebook.value('selectedEpoch')
  const selectedDate = await notebook.value('selectedDate')
  console.log('Date:', selectedDate)
  let count = 0
  while (true) {
    const asks = await notebook.value('asks')
    if (count++ % 100 === 0) {
      console.log(
        'Asks => State: ',
        asks.state,
        ' Elapsed: ',
        asks.elapsed,
        ' Records: ',
        asks.recordsLength
      )
    }
    if (asks.state === 'done') {
      jsonFilename = `asks-${selectedEpoch}.json`
      const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
      for (const record of asks.records) {
        await jsonFile.write(
          JSON.stringify({
            timestamp: selectedDate,
            epoch: selectedEpoch,
            ...record
          }) + '\n'
        )
      }
      jsonFile.on('finish', () => {
        fs.rename(`tmp/${jsonFilename}`, `input/asks/${jsonFilename}`, err => {
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
  await notebook.browser.close()
}
run()
