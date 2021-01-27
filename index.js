#!/usr/bin/env node

const AWS = require ('aws-sdk');
const zlib = require ('zlib');
const stream = require ('stream');

const MAX_CALL_RECORDS = 500;

class FirehoseWriteStream extends stream.Writable {
    constructor (deliveryStreamName, options) {
        super(options);

        this.firehose = new AWS.Firehose({apiVersion: '2015-08-04'});
        this.firehoseParams = { DeliveryStreamName: deliveryStreamName, Records: [] };
        this.recordsCount = 0;
        this.buf = ""
    }

    putLargeRecordBatch = (records, cb) => {
        if (records.length > MAX_CALL_RECORDS) {
            this.firehoseParams.Records = records.slice(0, MAX_CALL_RECORDS);
            let recordsLeft = records.slice(MAX_CALL_RECORDS);
            this.firehose.putRecordBatch(this.firehoseParams, (err, data) => {
                if (err) {
                    console.log("Error writing to Firehose: " + err + err.stack);
                    cb(new Error("writing to Firehose"));
                } else {
                    this.putLargeRecordBatch(recordsLeft, cb)
                }
            });
        } else if (records.length > 0 ){
            this.firehoseParams.Records = records.slice();
            this.firehose.putRecordBatch(this.firehoseParams, (err, data) => {
                if (err) {
                    console.log("Error writing to Firehose: " + err + err.stack);
                    cb(new Error("writing to Firehose"));
                } else {
                    cb();
                }
            });
        } else {
            cb();
        }

    };

    _write = (chunk, enc, cb) => {
        let tmp = (this.buf + chunk.toString()).split("\n");
        let lines = tmp.slice(0, -1);
        this.buf = tmp[tmp.length - 1]; // Since it's a stream, last line might be incomplete

        let records = [];
        lines.forEach(record => {
            records.push({'Data': record});
        });

        this.recordsCount += records.length;
        this.putLargeRecordBatch(records, cb)
    };

    _final = (cb) => {
        if (this.buf !== "") { // We purge the last line, if present
            this.recordsCount++;
            this.putLargeRecordBatch([{'Data': this.buf}], cb);
        } else {
            cb();
        }
    };
}

exports.handler = async (event) => {
    const firehoseStreamName = process.env.FIREHOSE_STREAM_NAME;
    let s3 = new AWS.S3({apiVersion: '2006-03-01'});

    let firehoseStream = new FirehoseWriteStream(firehoseStreamName, {highWaterMark: 512*1024});
    let noopTransform = new stream.Transform({
        transform(chunk, encoding, done) {
            done(null, chunk)
        }
    });

    let writers = [];
    for (let record of event.Records) {
        const bucketName = record.s3.bucket.name;
        const objectKey = record.s3.object.key;

        let objectStream = s3.getObject({Bucket: bucketName, Key: objectKey}).createReadStream();
        let transform = objectKey.endsWith(".gz") ? zlib.createGunzip() : noopTransform;

        writers.push(
            new Promise((res, rej) => {
                stream.pipeline(
                    objectStream,
                    transform,
                    firehoseStream,
                    (err, val) => {
                        if (err) {
                            rej("error: "+err);
                        } else {
                            res("success! Wrote "+firehoseStream.recordsCount + " records.");
                        }
                    }
                );
            })
        );
    }

    return Promise.all(writers);
};