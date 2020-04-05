#!/usr/bin/env node

const AWS = require ('aws-sdk');
const zlib = require ('zlib');
const readline = require ('readline');

const writeStreamToFirehose = async (firehose, deliveryStreamName, lineStream, bufferSize) => {
    return new Promise((resolve, reject) => {
        let firehoseParams = { DeliveryStreamName: deliveryStreamName, Records: [] };
        const onFirehoseError = (err, data) => {if (err) console.log(err, err.stack)};

        let recordsCount = 0;
        lineStream.on('line', (line) => {
            recordsCount++;
            firehoseParams.Records.push({'Data': line});
            if (firehoseParams.Records.length > bufferSize) {
                firehose.putRecordBatch(firehoseParams, onFirehoseError);
                firehoseParams.Records = [];
            }
        });

        lineStream.on('close', () => {
            firehose.putRecordBatch(firehoseParams, onFirehoseError);
            resolve("processed "+ recordsCount +" records");
        });

        lineStream.on('error', (err) => {
            reject(err);
        });
    });
};

exports.handler = async (event) => {
    const firehoseStreamName = process.env.FIREHOSE_STREAM_NAME;
    const bufferSize = 1000; // Numbers of lines to process at a time

    let s3 = new AWS.S3({apiVersion: '2006-03-01'});
    let firehose = new AWS.Firehose({apiVersion: '2015-08-04'});

    let writers = [];
    for (let record of event.Records) {
        const bucketName = record.s3.bucket.name;
        const objectKey = record.s3.object.key;

        let objectStream = s3.getObject({Bucket: bucketName, Key: objectKey}).createReadStream();
        if (objectKey.endsWith(".gz")) objectStream = objectStream.pipe(zlib.createGunzip());

        let lineStream = readline.createInterface({input: objectStream});
        writers.push(writeStreamToFirehose(firehose, firehoseStreamName, lineStream, bufferSize));
    }

    return Promise.all(writers);
};
