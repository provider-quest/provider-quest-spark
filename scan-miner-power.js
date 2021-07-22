const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/miner-report-miner-power-scanner',
    ['minerPower', 'selectedDate', 'minerCount']
    // { headless: false }
  )
  const selectedEpoch = await notebook.value('selectedEpoch')
  const selectedDate = await notebook.value('selectedDate')
  console.log('Date:', selectedDate)
  const tipSet = await notebook.value('tipSet')
  let count = 0
  let minerCount
  let numRecords = 0
  while (true) {
    const minerPower = await notebook.value('minerPower')
    if (minerPower.state === 'paused') {
      await notebook.redefine('start', 1)
      if (process.argv[2] === '--newest-not-recent') {
        await notebook.redefine('maxElapsed', 3 * 60 * 1000)
        await notebook.redefine('subsetToScan', 'Newest miners, not recent')
      } else if (process.argv[2] === '--all-not-recent') {
        await notebook.redefine('maxElapsed', 3 * 60 * 1000)
        await notebook.redefine('subsetToScan', 'All miners, not recent')
      } else {
        await notebook.redefine('maxElapsed', 5 * 60 * 1000)
      }
      await delay(1000)
      minerCount = await notebook.value('minerCount')
      continue
    }
    if (count++ % 100 === 0) {
      console.log(
        `Miner Power${process.argv[2] ? ' ' + process.argv[2] : ''} => State:`,
        minerPower.state,
        minerPower.elapsed ? `Elapsed: ${dateFns.formatDistance(minerPower.elapsed * 1000, 0)}` : '',
        'Counter:',
        `${minerPower.counter} / ${minerCount}`,
        'Records:',
        minerPower.recordsLength
      )
    }
    if (minerPower.state === 'done') {
      jsonFilename = `power-${selectedEpoch}.json`
      const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
      numRecords = minerPower.records.length
      for (const record of minerPower.records) {
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
        fs.rename(`tmp/${jsonFilename}`, `input/miner-power/${jsonFilename}`, err => {
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
  console.log('Records:', numRecords)
  await notebook.browser.close()
}
run()
