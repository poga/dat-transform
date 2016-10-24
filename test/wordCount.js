const dt = require('..')
const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const fs = require('fs')

var drive = hyperdrive(memdb())
var source = drive.createArchive()

fs.createReadStream('test/words.txt').pipe(source.createFileWriteStream('words.txt'))

source.finalize(() => {
  var drive2 = hyperdrive(memdb())
  var peer = drive2.createArchive(source.key, {sparse: true})
  replicate(source, peer)

  var result = dt.RDD(peer)
    .splitBy(/[\n\s]/)
    .filter(x => x !== '')
    .map(word => dt.kv(word, 1))

  tape('word count', function (t) {
    result.reduceByKey((x, y) => x + y)
      .toArray(res => {
        t.same(res, [{bar: 2, baz: 1, foo: 1}])
        t.end()
      })
  })
})

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}
