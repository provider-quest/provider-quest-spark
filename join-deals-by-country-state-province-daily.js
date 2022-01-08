const fs = require('fs')
const glob = require('glob-promise')

async function run () {
  const records = {}

  const aggrFiles = await glob(
    '../work/output/deals/by_provider_country_state_province/aggr_daily/json/part-*'
  )
  for (aggrFile of aggrFiles) {
    const lines = fs.readFileSync(aggrFile, 'utf8').split('\n')
    for (line of lines) {
      if (line === '') continue
      try {
        const { date, region, window, ...rest } = JSON.parse(line)
        const key = `${date}_${region}`
        records[key] = { date, region, ...rest }
        // console.log(aggrFile, key, records[key])
      } catch (e) {
        console.error('Json parse error', aggrFile, e)
        process.exit(1)
      }
    }
  }

  const sumFiles = await glob(
    '../work/output/deals/by_provider_country_state_province/sum_aggr_daily/json/part-*'
  )
  for (sumFile of sumFiles) {
    const lines = fs.readFileSync(sumFile, 'utf8').split('\n')
    for (line of lines) {
      if (line === '') continue
      try {
        const { date, region, ...rest } = JSON.parse(line)
        const key = `${date}_${region}`
        if (!records[key]) continue
        records[key] = { ...records[key], ...rest }
        // console.log(sumFile, key, records[key])
      } catch (e) {
        console.error('Json parse error', sumFile, e)
        process.exit(1)
      }
    }
  }

  sortedRecords = Object.values(records).sort((a, b) => {
    const dateCompare = a.date.localeCompare(b.date)
    if (dateCompare !== 0) return dateCompare
    return a.region.localeCompare(b.region)
  })

  console.log(JSON.stringify(sortedRecords, null, 2))
}

run()
