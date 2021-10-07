const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/miner-report-storage-ask-scanner',
    ['asks', 'selectedDate', 'minerCount']
    // { headless: false }
  )
  const selectedEpoch = await notebook.value('selectedEpoch')
  const selectedDate = await notebook.value('selectedDate')
  console.log('Date:', selectedDate)
  let count = 0
  let minerCount
  while (true) {
    const asks = await notebook.value('asks')
    if (asks.state === 'paused') {
      await notebook.redefine('start', 1)
      if (process.argv[2] === '--no-recents') {
        await notebook.redefine('maxElapsed', 3 * 60 * 1000)
        await notebook.redefine('subsetToScan', 'No recents')
      } else {
        await notebook.redefine('maxElapsed', 3 * 60 * 1000)
      }
      await delay(1000)
      minerCount = await notebook.value('minerCount')
      continue
    }
    if (count++ % 100 === 0) {
      console.log(
        `Asks${process.argv[2] ? ' ' + process.argv[2] : ''} => State:`,
        asks.state,
        asks.elapsed ? `Elapsed: ${dateFns.formatDistance(asks.elapsed * 1000, 0)}` : '',
        'Counter:',
        `${asks.counter} / ${minerCount}`,
        'Records:',
        asks.recordsLength,
        'Errors:',
        asks.errors
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
