import { S3Client, ListObjectsCommand } from "@aws-sdk/client-s3"

try {
  const s3Client = new S3Client({
    region: 'us-east-2'
  })
  const bucketParams = {
    Bucket: 'lily-data',
    Prefix: 'data/',
    Delimiter: '/'
  }
  const data = await s3Client.send(new ListObjectsCommand(bucketParams))
  const ranges = data.CommonPrefixes.map(({ Prefix: prefix }) => {
    const match = prefix.match(/^data\/(\d+)_+(\d+)\/$/)
    return { from: Number(match[1]), to: Number(match[2]) }
  }).sort(({ from: a }, { from: b }) => a - b)
  for (const range of ranges) {
    console.log(range)
  }
} catch (err) {
  console.error("Error", err)
}
