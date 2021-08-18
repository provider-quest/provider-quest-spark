const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')

fs.mkdirSync('input/miner-regions', { recursive: true })
fs.mkdirSync('input/miner-locations', { recursive: true })

async function run () {
  const notebook = await load(
    '@jimpick/miner-report-miner-to-region-mapper',
    ['minerRegionsTable', 'minerLocationsTable', 'currentEpoch', 'regionHierarchy']
    // { headless: false }
  )
  const epoch = await notebook.value('currentEpoch')
  const minerRegions = await notebook.value('minerRegionsTable')
  const minerLocations = await notebook.value('minerLocationsTable')
  const regionHierarchy = await notebook.value('regionHierarchy')
  await notebook.browser.close()

  async function writeMinerRegions () {
    fs.mkdirSync(`input/miner-regions/${epoch}`, { recursive: true })
    const jsonFilename = `miner-regions-${epoch}.json`
    const dest =`input/miner-regions/${epoch}/${jsonFilename}` 
    if (fs.existsSync(dest)) {
      console.log(`File already exists, skipping. ${jsonFilename}`)
    } else {
      const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
      for (const record of minerRegions) {
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
      console.log('Epoch:', epoch)
      console.log('Records:', minerRegions.length)
    }
  }

  async function writeMinerLocations () {
    fs.mkdirSync(`input/miner-locations/${epoch}`, { recursive: true })
    const jsonFilename = `miner-locations-${epoch}.json`
    const dest =`input/miner-locations/${epoch}/${jsonFilename}` 
    if (fs.existsSync(dest)) {
      console.log(`File already exists, skipping. ${jsonFilename}`)
    } else {
      const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
      for (const record of minerLocations) {
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
      console.log('Epoch:', epoch)
      console.log('Records:', minerLocations.length)
    }
  }

  async function writeRegionHierarchy () {
    fs.mkdirSync(`input/region-hierarchy/${epoch}`, { recursive: true })
    const jsonFilename = `region-hierarchy-${epoch}.json`
    const dest =`input/region-hierarchy/${epoch}/${jsonFilename}` 
    if (fs.existsSync(dest)) {
      console.log(`File already exists, skipping. ${jsonFilename}`)
    } else {
      fs.writeFileSync(`tmp/${jsonFilename}`,
                       JSON.stringify(regionHierarchy, null, 2))
      fs.rename(`tmp/${jsonFilename}`, dest, err => {
        if (err) {
          console.error('Error', err)
          process.exit(1)
        }
      })
      console.log('Filename:', jsonFilename)
      console.log('Epoch:', epoch)
    }
  }

  await writeMinerRegions()
  await writeMinerLocations()
  await writeRegionHierarchy()

  await notebook.browser.close()
}
run()
