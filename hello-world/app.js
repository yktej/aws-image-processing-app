const AWS = require('aws-sdk');
const S3 = new AWS.S3();
const DynamoDB = new AWS.DynamoDB.DocumentClient();
const SNS = new AWS.SNS();
const sharp = require('sharp');
const { randomUUID } = require('crypto');

const TABLE_NAME = 'ImageMetadata';
const TOPIC_ARN = process.env.TOPIC_ARN;

exports.handler = async (event) => {
  for (const record of event.Records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

    if (!key.match(/\.(jpg|jpeg|png)$/i)) {
      console.log(`Skipped non-image file: ${key}`);
      continue;
    }

    try {
      const originalImage = await S3.getObject({
        Bucket: bucket,
        Key: key,
      }).promise();
      const originalSize = originalImage.Body.length;

      const resizedBuffer = await sharp(originalImage.Body)
        .resize({ width: 500 })
        .toBuffer();

      const resizedKey = `resized/${key.split('/').pop()}`;
      await S3.putObject({
        Bucket: bucket,
        Key: resizedKey,
        Body: resizedBuffer,
        ContentType: 'image/jpeg',
      }).promise();

      const metadata = {
        id: randomUUID(),
        originalFileName: key,
        resizedFileName: resizedKey,
        uploadTimestamp: new Date().toISOString(),
        originalSize,
        resizedSize: resizedBuffer.length,
      };

      await DynamoDB.put({
        TableName: TABLE_NAME,
        Item: metadata,
      }).promise();

      const message = `
New image processed:
- Original: ${key}
- Resized: ${resizedKey}
- Time: ${metadata.uploadTimestamp}
      `;

      await SNS.publish({
        TopicArn: TOPIC_ARN,
        Subject: 'Image Upload Processed',
        Message: message,
      }).promise();

      console.log(`Successfully processed and notified for: ${key}`);
    } catch (error) {
      console.error(`Failed to process ${key}:`, error);
    }
  }
};
