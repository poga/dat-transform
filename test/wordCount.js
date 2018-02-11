'use strict'
const dt = require('..')
const test = require('tap').test
const { createSparsePeer } = require('./util/testDrive')
const toPromise = require('./util/toPromise')

test('word count', t =>
  createSparsePeer(null, {
    files: {
      'words.txt': null,
      'words2.txt': 'words.txt'
    }
  })
  .then(peer =>
    toPromise(
      dt.RDD(peer)
        .splitBy(/[\n\s]/)
        .filter(x => x !== '')
        .map(word => dt.kv(word, 1))
        .reduceByKey((x, y) => x + y)
    )
  )
  .then(res => t.same(res, [{bar: 4, baz: 2, foo: 2}]))
)
