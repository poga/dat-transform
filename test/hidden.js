const dt = require('..')
const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const test = require('tap').test
const fs = require('fs')

var drive = hyperdrive(memdb())
var source = drive.createArchive()

fs.createReadStream('test/test.csv').pipe(source.createFileWriteStream('test.csv'))
fs.createReadStream('test/test2.csv').pipe(source.createFileWriteStream('/abc/.test2.csv'))

source.finalize(() => {
  var drive2 = hyperdrive(memdb())
  var peer = drive2.createArchive(source.key, {sparse: true})
  replicate(source, peer)

  var result = dt.RDD(peer)
    .csv()
    .map(row => parseInt(row['value'], 10))

  test('hidden file should be ignored', function (t) {
    result.collect()
      .toArray(res => {
        t.same(res, [1, 2, 3, 4, 5])
        t.end()
      })
  })
})

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}
