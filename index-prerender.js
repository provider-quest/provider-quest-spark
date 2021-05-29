const { load } = require("@alex.garcia/observable-prerender");
(async () => {
  const notebook = await load("@jimpick/lotus-js-client-space-race-miner-power-scanner", ["minerPower", "selectedDate"], { headless: false });
  const selectedEpoch = await notebook.value('selectedEpoch')
  const selectedDate = await notebook.value('selectedDate')
  console.log('Date:', selectedDate)
  const tipSet = await notebook.value('tipSet')
  while (true) {
    const minerPower = await notebook.value('minerPower')
    console.log('State: ', minerPower.state, ' Elapsed: ', minerPower.elapsed, ' Records: ', minerPower.records && minerPower.records.length)
    if (minerPower.state === 'done') break
  }
  console.log('Epoch:', selectedEpoch)
  console.log('Date:', selectedDate)
  console.log('TipSet:', tipSet)
  await notebook.browser.close()
})();
