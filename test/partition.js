'use strict'
const dt = require('..')
const test = require('tap').test
const { createDrive, createSparsePeer } = require('./util/testDrive')
const toPromise = require('./util/toPromise')

createSparsePeer().then(peer => {
  var result = dt.RDD(peer)
    .csv()
    .map(row => parseInt(row['value'], 10))

  test('partition', t =>
    result
      .map(x => [x % 2, x])
      .partitionByKey(createDrive())
      .then(newDt => toPromise(newDt.collect()))
      .then(x => t.same(x.map(b => b.toString()).join(''), '1\n3\n5\n7\n9\n2\n4\n6\n8\n10\n', 'should contain both partitions after another'))
  )

  test('get', t =>
    result
      .map(x => [x % 2, x])
      .partitionByKey(createDrive())
      .then(newDt => toPromise(newDt.get('0').collect()))
      .then(x => t.same(x.map(b => b.toString()).join(''), '2\n4\n6\n8\n10\n', 'should contain only 0 partition'))
  )

  test('select', t =>
    result
      .map(x => [x % 3, x])
      .partitionByKey(createDrive())
      .then(newDt => toPromise(
        newDt
          .select(x => parseInt(x, 10) < 2) // x % 3 < 2
          .collect()
      ))
      .then(x => t.same(x.map(b => b.toString()).join(''), '1\n4\n7\n10\n3\n6\n9\n', 'should match the selection after the partition'))
  )

  test('get only works on RDD before transform', t => {
    t.throws(() => result.get('test.csv'))
    t.end()
  })

  test('select only works on RDD before transform', t => {
    t.throws(() => result.select(x => x.name === 'test.csv'))
    t.end()
  })
})
