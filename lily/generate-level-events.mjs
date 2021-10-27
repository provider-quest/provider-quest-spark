import fs from 'fs'
import { scalePow } from 'd3-scale'

// See: https://observablehq.com/@jimpick/d3-scale-for-provider-power-provider-quest?collection=@jimpick/provider-quest

const contents = fs.readFileSync(process.argv[2])

const powerData = JSON.parse(contents)

const maxPower = 166253442384265200

const powerScale = scalePow()
  .exponent(0.25)
  .domain([0, maxPower])
  .range([0, 15])

const powerLevels = powerData.map(({ date, qualityAdjPower }) => ({ date, qualityAdjPower, level: Math.round(powerScale(qualityAdjPower)) }))

const transitions = powerLevels.reduce((acc, record) => {
  const lastLevel = acc.length ? acc[acc.length - 1].level : -1
  const newAcc = [...acc]
  if (record.level > lastLevel) newAcc.push(record)
  return newAcc
}, [])

console.log(JSON.stringify(transitions))
