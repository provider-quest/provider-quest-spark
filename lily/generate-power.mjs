import fs from 'fs'
import { epochToDate, dateToEpoch } from './filecoin-epochs.mjs'

const baseDir = 'sync/power-actor-claims'
const files = fs.readdirSync(baseDir)
const outputDir = 'generated/miner-power'
fs.mkdirSync(outputDir, { recursive: true })

const ranges = files
  .map(file => {
    const match = file.match(/^(\d+)__(\d+)\.csv/)
    // console.log(file, match)
    return { from: Number(match[1]), to: Number(match[2]) }
  })
  .sort(({ from: a }, { from: b }) => a - b)

const startDate = epochToDate(0)
let nextEpoch = 120 * 2 // first date boundary, 2 hours after genesis

const fileCursorIndex = 0

const providerPower = {}
let previousProviderPower = {}

let counter = 0

// ranges.length = 7 // for testing

for (const range of ranges) {
  const { from, to } = range
  console.log('Jim processing', from, to)
  const file = `${baseDir}/${from}__${to}.csv`
  const contents = fs.readFileSync(file, 'utf8')
  const lines = contents.split('\n').filter(line => line.length)
  const records = lines
    .map(line => {
      // 823920,f0131822,bafy2bzacea2syvwqcl4umygrwhlr7zclgy4e4gmmr5ranpfqrfp2y6qu6zika,75114579960528896,75114579960528896
      // console.log('Jim line', line)
      const cells = line.split(',')
      const epoch = Number(cells[0])
      const provider = Number(cells[1].slice(1))
      const rawBytePower = Number(cells[3])
      const qualityAdjPower = Number(cells[4])
      return { epoch, provider, rawBytePower, qualityAdjPower }
    })
    .sort(({ epoch: a }, { epoch: b }) => a - b)
  for (const record of records) {
    while (record.epoch > nextEpoch) {
      emit()
      nextEpoch += 24 * 60 * 2 // 2 epochs per minute
      counter = 0
    }
    providerPower[record.provider] = record
    counter++
    // console.log(`${from}-${to}`, record)
  }
}
emit()

function emit () {
  console.log(nextEpoch, epochToDate(nextEpoch), counter)
  const timestamp = epochToDate(nextEpoch)
  let output = ''
  for (const provider in providerPower) {
    if (
      (!previousProviderPower[provider] &&
        (providerPower[provider].rawBytePower > 0 ||
          providerPower[provider].qualityAdjPower > 0)) ||
      (previousProviderPower[provider] &&
        !(
          providerPower[provider].rawBytePower === 0 &&
          previousProviderPower[provider].rawBytePower === 0 &&
          providerPower[provider].qualityAdjPower === 0 &&
          previousProviderPower[provider].qualityAdjPower === 0
        ))
    ) {
      const outputRecord = {
        timestamp,
        epoch: nextEpoch,
        miner: `f0${provider}`,
        rawBytePower: providerPower[provider].rawBytePower,
        qualityAdjPower: providerPower[provider].qualityAdjPower
      }
      output += JSON.stringify(outputRecord) + '\n'
      //console.log(nextEpoch, `f0${provider}`, providerPower[provider])
    }
  }
  const outputFile =`${outputDir}/power-${nextEpoch}-lily.json` 
  fs.writeFileSync(outputFile, output)
  console.log(`Wrote ${outputFile}`)
  previousProviderPower = { ...providerPower }
}
