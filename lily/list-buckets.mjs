import { S3Client, ListBucketsCommand } from "@aws-sdk/client-s3"

try {
  const s3Client = new S3Client()
  const data = await s3Client.send(new ListBucketsCommand({}))
  console.log(data.Buckets)
} catch (err) {
  console.error("Error", err)
}
