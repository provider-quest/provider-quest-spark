const fs = require('fs')

const dirs = fs.readdirSync('estuary-archive')

const unsortedFiles = []

for (const dir of dirs) {
  if (dir === 'archive.sh') continue
  const base = `estuary-archive/${dir}`
  const files = fs.readdirSync(base)
  for (const file of files) {
    if (file.match(/-\d+\.json$/)) {
      unsortedFiles.push(`${base}/${file}`)
    }
  }
}

const sortedFiles = unsortedFiles.sort((a, b) => {
  const numA = Number(a.match(/-(\d+)\.json$/)[1])
  const numB = Number(b.match(/-(\d+)\.json$/)[1])
  const compare = numA - numB
  if (compare !== 0) {
    return compare
  } else {
    return a.localeCompare(b)
  }
})

const epochs = process.argv[2] && Number(process.argv[2])
// console.log('Epochs', epochs)

let filteredFiles = sortedFiles

if (epochs) {
  const lastFile = filteredFiles[filteredFiles.length - 1]
  const lastEpoch = Number(lastFile.match(/-(\d+)\.json$/)[1])
  const firstEpoch = lastEpoch - epochs
  filteredFiles = filteredFiles.filter(file => {
    const epoch = Number(file.match(/-(\d+)\.json$/)[1])
    return epoch >= firstEpoch
  })
}

for (const file of filteredFiles) {
  console.log(file)
}
