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
  var result = dt.RDD(source)

  // csv
  test('marshal csv', function (t) {
    t.same(result.csv().marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'csv', params: null, paramsType: undefined}
    ])
    t.end()
  })

  test('unmarshal csv', function (t) {
    var json = JSON.stringify(result.csv().marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x.map(b => b.value), ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
        t.end()
      })
  })

  // splitBy
  test('marshal splitBy', function (t) {
    t.same(result.splitBy(/\s/).marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'splitBy', params: '\\s', paramsType: 'regexp'}
    ])
    t.end()
  })

  test('unmarshal splitBy', function (t) {
    var json = JSON.stringify(result.splitBy(/\n/).marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x, ['value', '1', '2', '3', '4', '5', '', 'value', '6', '7', '8', '9', '10', ''])
        t.end()
      })
  })

  // map
  test('marshal map', function (t) {
    var f = x => x + 1
    t.same(result.map(f).marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'map', params: f.toString(), paramsType: undefined}
    ])
    t.end()
  })

  test('unmarshal map', function (t) {
    var json = JSON.stringify(result.csv().map(row => parseInt(row['value'], 10)).marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        t.end()
      })
  })

  // filter
  test('marshal filter', function (t) {
    var f = x => x === 1
    t.same(result.filter(f).marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'filter', params: f.toString(), paramsType: undefined}
    ])
    t.end()
  })

  test('unmarshal filter', function (t) {
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

  // get
  test('marshal get', function (t) {
    t.same(result.get('test.csv').marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'get', params: 'test.csv', paramsType: undefined}
    ])
    t.end()
  })

  test('unmarshal get', function (t) {
    var json = JSON.stringify(
      result
        .get('test.csv')
        .marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x.toString(), 'value\n1\n2\n3\n4\n5\n')
        t.end()
      })
  })

  // select
  test('marshal select', function (t) {
    var selector = x => x.name === 'test.csv'
    t.same(result.select(selector).marshal(), [
      {type: 'parent', key: source.key.toString('hex')},
      {type: 'select', params: selector.toString(), paramsType: undefined}
    ])
    t.end()
  })

  test('unmarshal select', function (t) {
    var json = JSON.stringify(
      result
        .select(x => x.name === 'test.csv')
        .marshal())
    dt.unmarshal(drive, json)
      .collect()
      .toArray(x => {
        t.same(x.toString(), 'value\n1\n2\n3\n4\n5\n')
        t.end()
      })
  })
})
