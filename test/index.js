const dt = require('..')
const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const test = require('tap').test
const fs = require('fs')

var drive = hyperdrive(memdb())
var source = drive.createArchive()

fs.createReadStream('test/test.csv').pipe(source.createFileWriteStream('test.csv'))
fs.createReadStream('test/test2.csv').pipe(source.createFileWriteStream('test2.csv'))

source.finalize(() => {
  var drive2 = hyperdrive(memdb())
  var peer = drive2.createArchive(source.key, {sparse: true})
  replicate(source, peer)

  var result = dt.RDD(peer)
    .csv()
    .map(row => parseInt(row['value'], 10))
    .map(x => x * 2)
    .map(x => x + 1)

  // since transform is applied to each file, we won't get correct data
  test('sort', function (t) {
    result.sortBy((x, y) => y - x)
      .collect()
      .toArray(res => {
        t.same(res, [11, 9, 7, 5, 3, 21, 19, 17, 15, 13])
        t.end()
      })
  })

  // if we want to sort the whole data, use takeSortBy
  test('takeSortedBy', function (t) {
    result
      .takeSortedBy((x, y) => y - x)
      .toArray(res => {
        t.same(res, [21, 19, 17, 15, 13, 11, 9, 7, 5, 3])
        t.end()
      })
  })

  test('count', function (t) {
    result
      .count()
      .toArray(res => {
        t.same(res, [10])
        t.end()
      })
  })

  test('sum', function (t) {
    result
      .sum()
      .toArray(res => {
        t.same(res, [120])
        t.end()
      })
  })

  test('take 1', function (t) {
    result.take(1)
      .toArray(res => {
        t.same(res, [3])
        t.end()
      })
  })

  test('take all', function (t) {
    result.collect()
      .sortBy((a, b) => { return a - b })
      .toArray(res => {
        t.same(res, [3, 5, 7, 9, 11, 13, 15, 17, 19, 21])
        t.end()
      })
  })

  test('transform can be reused', function (t) {
    result
      .take(2)
      .toArray(res => {
        t.same(res, [3, 5])
        result.take(1).toArray(res => {
          t.same(res, [3])
          t.end()
        })
      })
  })

  test('transform can be used concurrently', function (t) {
    var c = 0
    result.take(2).toArray(res => {
      t.same(res, [3, 5])
      c += 1
      if (c === 2) done()
    })
    result.take(1).toArray(res => {
      t.same(res, [3])
      c += 1
      if (c === 2) done()
    })

    function done () { t.end() }
  })
})

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}
