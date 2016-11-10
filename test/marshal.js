const dt = require('..')
const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const fs = require('fs')

var drive = hyperdrive(memdb())
var source = drive.createArchive()

fs.createReadStream('test/test.csv').pipe(source.createFileWriteStream('test.csv'))
fs.createReadStream('test/test2.csv').pipe(source.createFileWriteStream('test2.csv'))

source.finalize(() => {
  var result = dt.RDD(source)

  // csv
  tape('marshal csv', function (t) {
    t.same(result.csv().marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'csv', params: null, paramsType: undefined}
    ])
    t.end()
  })

  tape('unmarshal csv', function (t) {
    var json = JSON.stringify(result.csv().marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x.map(b => b.value), ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
        t.end()
      })
  })

  // splitBy
  tape('marshal splitBy', function (t) {
    t.same(result.splitBy(/\s/).marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'splitBy', params: '\\s', paramsType: 'regexp'}
    ])
    t.end()
  })

  tape('unmarshal splitBy', function (t) {
    var json = JSON.stringify(result.splitBy(/\n/).marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x, ['value', '1', '2', '3', '4', '5', '', 'value', '6', '7', '8', '9', '10', ''])
        t.end()
      })
  })

  // map
  tape('marshal map', function (t) {
    var f = x => x + 1
    t.same(result.map(f).marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'map', params: f.toString(), paramsType: undefined}
    ])
    t.end()
  })

  tape('unmarshal map', function (t) {
    var json = JSON.stringify(result.csv().map(row => parseInt(row['value'], 10)).marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        t.end()
      })
  })

  // filter
  tape('marshal filter', function (t) {
    var f = x => x === 1
    t.same(result.filter(f).marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'filter', params: f.toString(), paramsType: undefined}
    ])
    t.end()
  })

  tape('unmarshal filter', function (t) {
    var json = JSON.stringify(
      result.csv()
        .map(row => parseInt(row['value'], 10))
        .filter(x => x % 2 === 0)
        .marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x, [2, 4, 6, 8, 10])
        t.end()
      })
  })
})
