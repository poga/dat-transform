'use strict'
const dt = require('..')
const test = require('tap').test
const { createSparsePeer } = require('./util/testDrive')
const toPromise = require('./util/toPromise')

test('hidden file should be ignored', t =>
  createSparsePeer(null, {
    files: {
      'test.csv': null,
      '/abc/.test2.csv': 'test2.csv'
    }
  })
    .then(peer => toPromise(
      dt.RDD(peer)
        .csv()
        .map(row => parseInt(row.value, 10))
        .collect()
    ))
    .then(res => t.same(res, [1, 2, 3, 4, 5]))
)
