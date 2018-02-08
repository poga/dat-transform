'use strict'
const dt = require('..')
const test = require('tap').test
const { createSparsePeer } = require('./util/testDrive')
const toPromise = require('./util/toPromise')

createSparsePeer().then(peer => {
  var result = dt.RDD(peer)
    .csv()
    .map(row => parseInt(row['value'], 10))
    .map(x => x * 2)
    .map(x => x + 1)

  // since transform is applied to each file, we won't get correct data
  test('sort', t =>
    toPromise(result
      .sortBy((x, y) => y - x)
      .collect()
    )
      .then(res => t.deepEquals(res, [11, 9, 7, 5, 3, 21, 19, 17, 15, 13]))
  )

  // if we want to sort the whole data, use takeSortBy
  test('takeSortedBy', t =>
    toPromise(result.takeSortedBy((x, y) => y - x))
      .then(res => t.same(res, [21, 19, 17, 15, 13, 11, 9, 7, 5, 3]))
  )

  test('count', t =>
    toPromise(result.count())
      .then(res => t.same(res, [10]))
  )

  test('sum', t =>
    toPromise(result.sum())
      .then(res => t.same(res, [120]))
  )

  test('take 1', t =>
    toPromise(result.take(1))
      .then(res => t.same(res, [3]))
  )

  test('take all', t =>
    toPromise(result.collect().sortBy((a, b) => a - b))
      .then(res => t.same(res, [3, 5, 7, 9, 11, 13, 15, 17, 19, 21]))
  )

  test('transform can be reused', t =>
    toPromise(result.take(2))
      .then(res => t.same(res, [3, 5]))
      .then(() => toPromise(result.take(1)))
      .then(res => t.same(res, [3]))
  )

  test('transform can be used concurrently', t =>
    Promise.all([
      toPromise(result.take(2))
        .then(res => t.same(res, [3, 5])),
      toPromise(result.take(1))
        .then(res => t.same(res, [3]))
    ])
  )
})
