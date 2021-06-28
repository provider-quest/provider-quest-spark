const fs = require('fs')
const { formatWithOptions } = require('util')
const { load } = require('@alex.garcia/observable-prerender')
const dateFns = require('date-fns')
const delay = require('delay')

fs.mkdirSync('input/deals', { recursive: true })
fs.mkdirSync('local-state/deals', { recursive: true })

let startHeight

try {
  startHeight = Number(fs.readFileSync('local-state/deals/last-height', 'utf8')) + 1
} catch (e) {}

async function run () {
  let jsonFilename
  let lastHeight
  const notebook = await load(
    '@jimpick/miner-report-publish-deal-messages-stream',
    ['deals']
    // { headless: false }
  )
  // const selectedEpoch = await notebook.value('selectedEpoch')
  // const selectedDate = await notebook.value('selectedDate')
  // console.log('Date:', selectedDate)
  // const tipSet = await notebook.value('tipSet')
  let lastMsg = ''
  while (true) {
    const deals = await notebook.value('deals')
    if (deals.state === 'paused') {
      if (startHeight) {
        await notebook.redefine('selectedHeight', startHeight)
      }
      await notebook.redefine('start', 1)
      await delay(1000)
      continue
    }
    let newMsg = 'Deals => State: ' + deals.state
    if (deals.state === 'streaming') {
      newMsg = newMsg + ' ' + (
        deals.elapsed ? ` Elapsed: ${dateFns.formatDistance(deals.elapsed * 1000, 0)} - ` +
        `${deals.height}, ${deals.endHeight - deals.height} remaining ` +
        `(${deals.height - deals.startHeight} epochs, ` +
        `${deals.messagesProcessed} msgs, ` +
        `${deals.messageHits} hits, ` +
        `${deals.dealsLength} deals)`: '')
    }
    if (newMsg !== lastMsg) {
      console.log(newMsg)
      lastMsg = newMsg
    }
    
    if (deals.state === 'done') {
      lastHeight = deals.lastHeight
      if (deals.deals.length > 0 && lastHeight) {
        jsonFilename = `deals-${lastHeight}.json`
        const jsonFile = fs.createWriteStream(`tmp/${jsonFilename}`)
        for (const deal of deals.deals) {
          await jsonFile.write(JSON.stringify(deal) + '\n')
        }
        jsonFile.on('finish', () => {
          fs.rename(`tmp/${jsonFilename}`, `input/deals/${jsonFilename}`, err => {
            if (err) {
              console.error('Error', err)
              process.exit(1)
            }
          })
        })
        jsonFile.end()
      }
      break
    }
    await delay(1000)
  }
  console.log('Filename:', jsonFilename)
  console.log('Last Epoch:', lastHeight)
  if (lastHeight) {
    fs.writeFileSync('local-state/deals/last-height', `${lastHeight}`)
  }
  await notebook.browser.close()
}
run()
