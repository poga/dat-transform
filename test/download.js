'use strict'
const dt = require('..')
const test = require('tap').test
const { createSource, createSparsePeer, createDrive } = require('./util/testDrive')
const toPromise = require('./util/toPromise')

createSource().then(source => {
  test('partition will download everything', t =>
    createSparsePeer(source, {storeProgress: true})
      .then(peer =>
        dt.RDD(peer)
          .map(x => [x % 2, x])
          .partitionByKey(createDrive())
          .then(newDt => toPromise(newDt.collect()))
          .then(x => {
            t.same(x.map(b => b.toString()).join(''), 'value\n1\n2\n3\n4\n5\n\nvalue\n6\n7\n8\n9\n10\n\n', 'comparing data output, should contain both files')
            t.same(peer.downloaded, 33, 'all data should have been downloaded')
          })
      )
  )

  test('get only download requested file', t =>
    createSparsePeer(source, {storeProgress: true})
      .then(peer =>
        toPromise(
          dt.RDD(peer)
            .get('test.csv')
            .collect()
        )
        .then(x => {
          t.same(x.map(b => b.toString()), ['value\n1\n2\n3\n4\n5\n'], 'comparing the value to the contents of test.csv')
          t.same(peer.downloaded, 16, 'only the first file should have been downloaded')
        })
      )
  )

  test('select only download requested file', t =>
    createSparsePeer(source, {storeProgress: true})
      .then(peer =>
        toPromise(
          dt.RDD(peer)
            .get('test2.csv')
            .collect()
        )
        .then(x => {
          t.same(x.map(b => b.toString()), ['value\n6\n7\n8\n9\n10\n'], 'comparing the result to the contents of test2.csv')
          t.same(peer.downloaded, 17, 'only the second file should have been downloaded')
        })
      )
  )
})
