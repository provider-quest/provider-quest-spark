const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@jimpick/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')
const fastify = require('fastify')({ logger: true })
const fastifyStatic = require('fastify-static')
const fastifyCors = require('fastify-cors')
const workDir = process.env.WORK_DIR || '.'
const tmpDir = `${workDir}/tmp`

fastify.register(fastifyCors, {
  origin: '*'
})

fastify.register(fastifyStatic, {
  root: process.env.FUNDING_COLLECTOR_SINK_DIR + '/combined/results',
  prefix: '/funding/'
})

const startFastify = async () => {
  try {
    await fastify.listen(3000, '0.0.0.0')
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}
startFastify()

async function run () {
  const notebook = await load(
    '@jimpick/internal-provider-funding-tree-provider-quest-test-2',
    [
      'minersAndFundersUrl',
      'currentEpoch',
      'currentEpochDate',
      'funderTreeWithDelegatesProgressWithoutResult'
    ],
    { headless: true }
  )
  // await notebook.redefine('interactiveEpoch', 1451584) // Override
  if (process.env.FUNDING_COLLECTOR_SINK_DIR) {
    const fundingFiles = fs.readdirSync(`${process.env.FUNDING_COLLECTOR_SINK_DIR}/combined/results/`)
    const fundingFileDates = []
    for (const file of fundingFiles) {
      const match = file.match(/^miners-and-funders-(.*)\.json$/)
      if (match) {
        fundingFileDates.push(match[1])
      }
    }
    fundingFileDates.sort()
    if (fundingFileDates.length > 0) {
      const lastDate = fundingFileDates[fundingFileDates.length - 1]
      const minersAndFundersUrl = `http://127.0.0.1:3000/funding/miners-and-funders-${lastDate}.json`
      await notebook.redefine('minersAndFundersUrl', minersAndFundersUrl) // Override
      console.log('minersAndFundersUrl:', minersAndFundersUrl)
    }
  }
  const currentEpoch = await notebook.value('currentEpoch')
  const currentEpochDate = await notebook.value('currentEpochDate')
  // console.log('Date:', selectedDate)
  // const tipSet = await notebook.value('tipSet')
  /*
  let count = 0
  let minerCount
  let numRecords = 0
  */
  while (true) {
    try {
      const progress = await notebook.value('funderTreeWithDelegatesProgressWithoutResult')
      if (!progress) {
        await delay(1000)
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
      if (progress.done) {
        console.log('Done')
        const outputEpoch = currentEpoch

        await writeJson(
          'syntheticProviderCSPRegions',
          'synthetic-regions',
          'synthetic-provider-country-state-province'
        )
        await writeJson(
          'syntheticProviderRegions',
          'synthetic-regions',
          'synthetic-provider-regions'
        )
        await writeJson(
          'syntheticProviderCSPLocations',
          'synthetic-locations',
          'synthetic-provider-country-state-province-locations'
        )
        await writeJson(
          'syntheticProviderLocations',
          'synthetic-locations',
          'synthetic-provider-locations'
        )

        async function writeJson (cellName, dirName, filePrefix) {
          const cspRegions = await notebook.value(cellName)
          const jsonFilename = `${filePrefix}-${outputEpoch}.json`
          const outputTmpDir = process.env.SYNTHETIC_LOCATIONS_SINK_DIR ?
            `${process.env.SYNTHETIC_LOCATIONS_SINK_DIR}/tmp` : tmpDir

          fs.mkdirSync(outputTmpDir, { recursive: true })
          const jsonFile = fs.createWriteStream(`${outputTmpDir}/${jsonFilename}`)
          numRecords = cspRegions.length
          for (const record of cspRegions) {
            await jsonFile.write(JSON.stringify(record) + '\n')
          }
          jsonFile.on('finish', () => {
            const outputDir = process.env.SYNTHETIC_LOCATIONS_SINK_DIR ?
              `${process.env.SYNTHETIC_LOCATIONS_SINK_DIR}/${dirName}/${outputEpoch}` :
              `${workDir}/input/${dirName}/${outputEpoch}`
            fs.mkdirSync(outputDir, { recursive: true })
            fs.rename(`${outputTmpDir}/${jsonFilename}`, `${outputDir}/${jsonFilename}`, err => {
              if (err) {
                console.error('Error', err)
                process.exit(1)
              }
            })
            console.log(`Wrote ${jsonFilename}`)
          })
          jsonFile.end()
        }

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
  await notebook.browser.close()
  // console.log('Close')
  // await delay(10 * 60 * 1000) // 10 minutes
  fastify.close()
}
run()
