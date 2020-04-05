#!/usr/bin/env node

const stream = require('stream');
const readline = require ('readline');
const sinon = require ('sinon');
const AWS = require ('aws-sdk');
const chai = require ('chai');
const { writeStreamToFirehose } = require('../index');
const sinonChai = require("sinon-chai");

chai.should();
chai.use(sinonChai);

describe('writeStreamToFirehose', () => {
    it('should insert data in batches', () => {
        const testCases = [{
            lines: '123\n456\n789\n123\n456',
            bufferSize: 10,
            expect: 1
        },{
            lines: '123\n456\n789\n123\n456',
            bufferSize: 1,
            expect: 5
        },{
            lines: '123\n456\n789\n123\n456',
            bufferSize: 2,
            expect: 3
        }];

        for (let testCase of testCases) {
            var s = new stream.Readable();
            s.push(testCase.lines);
            s.push(null);

            let firehose = new AWS.Firehose({apiVersion: '2015-08-04'});
            let putRecordBatch = sinon.stub(firehose, 'putRecordBatch').callsFake((params, error) => {});

            let iStream = readline.createInterface({input: s});
            writeStreamToFirehose(firehose, 'test', iStream, testCase.bufferSize).then((res) => {
                putRecordBatch.should.have.been.callCount(testCase.expect)
            });
        }
    });


    it('should insert all records', () => {
        const testCases = [{
            lines: '123\n456\n789\n123\n456',
            expect: 5,
            bufferSize: 2
        },{
            lines: '123\n456\n789\n123\n456',
            expect: 5,
            bufferSize: 1
        },{
            lines: '123\n456\n789\n123\n456',
            expect: 5,
            bufferSize: 10
        }];

        for (let testCase of testCases) {
            var s = new stream.Readable();
            s.push(testCase.lines);
            s.push(null);

            let firehose = new AWS.Firehose({apiVersion: '2015-08-04'});
            sinon.stub(firehose, 'putRecordBatch').callsFake((params, error) => {});

            let iStream = readline.createInterface({input: s});
            writeStreamToFirehose(firehose, 'test', iStream, testCase.bufferSize).then((res) => {
                chai.expect(res).to.equal('processed '+testCase.expect+' records');
            });
        }
    });
});