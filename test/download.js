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
  test('partition will download everything', function (t) {
    var downloaded = 0
    var drive2 = hyperdrive(memdb())
    var peer = drive2.createArchive(source.key, {sparse: true})
    peer.on('download', data => { downloaded += data.length })
    replicate(source, peer)

    var result = dt.RDD(peer)

    var newArchive = drive2.createArchive()
    result
      .map(x => [x % 2, x])
      .partitionByKey(newArchive).then(next => {
        next
        .collect()
        .toArray(x => {
          t.same(x.map(b => b.toString()), ['value\n1\n2\n3\n4\n5\n\nvalue\n6\n7\n8\n9\n10\n\n'])
          t.same(downloaded, 33)
          t.end()
        })
      })
  })

  test('get only download requested file', function (t) {
    var downloaded = 0
    var drive2 = hyperdrive(memdb())
    var peer = drive2.createArchive(source.key, {sparse: true})
    peer.on('download', data => { downloaded += data.length })
    replicate(source, peer)

    var result = dt.RDD(peer)
    result.get('test.csv')
      .collect()
      .toArray(x => {
        t.same(x.map(b => b.toString()), ['value\n1\n2\n3\n4\n5\n'])
        t.same(downloaded, 16)
        t.end()
      })
  })

  test('select only download requested file', function (t) {
    var downloaded = 0
    var drive2 = hyperdrive(memdb())
    var peer = drive2.createArchive(source.key, {sparse: true})
    peer.on('download', data => { downloaded += data.length })
    replicate(source, peer)

    var result = dt.RDD(peer)
    result
      .select(x => x.name === 'test2.csv')
      .collect()
      .toArray(x => {
        t.same(x.map(b => b.toString()), ['value\n6\n7\n8\n9\n10\n'])
        t.same(downloaded, 17)
        t.end()
      })
  })
})

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}
