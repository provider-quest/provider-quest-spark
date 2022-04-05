const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')

const workDir = process.env.WORK_DIR || '.'
const tmpDir = `${workDir}/tmp`

async function run () {
  let jsonFilename
  const notebook = await load(
    '@jimpick/internal-provider-funding-tree-provider-quest-test',
    ['funderTreeWithDelegatesProgress'],
    { headless: false }
  )
  // await notebook.redefine('interactiveEpoch', 1451584) // Override
  // const selectedEpoch = await notebook.value('selectedEpoch')
  // const selectedDate = await notebook.value('selectedDate')
  // console.log('Date:', selectedDate)
  // const tipSet = await notebook.value('tipSet')
  /*
  let count = 0
  let minerCount
  let numRecords = 0
  */
  while (true) {
    try {
      const progress = await notebook.value('funderTreeWithDelegatesProgress')
      if (!progress) {
        await notebook.redefine('start', 1)
        await delay(1000)
        // minerCount = await notebook.value('minerCount')
        continue
      }
      /*
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
      */
      console.log('Progress', JSON.stringify(progress))
      if (progress.state === 'done') {
        /*
        jsonFilename = `power-${selectedEpoch}.json`
        const jsonFile = fs.createWriteStream(`${tmpDir}/${jsonFilename}`)
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
          fs.rename(`${tmpDir}/${jsonFilename}`, `${workDir}/input/miner-power/${jsonFilename}`, err => {
            if (err) {
              console.error('Error', err)
              process.exit(1)
            }
          })
        })
        jsonFile.end()
        */
        break
      }
    } catch (e) {
      console.error('Exception', e)
      break
    }
  }
  /*
  console.log('Filename:', jsonFilename)
  console.log('Epoch:', selectedEpoch)
  console.log('Date:', selectedDate)
  console.log('TipSet:', tipSet)
  console.log('Records:', numRecords)
  */
  // await notebook.browser.close()
  await delay(10 * 60 * 1000) // 10 minutes  
}
run()
