'use strict'
const dt = require('..')
const test = require('tap').test
const { createSource, createSparsePeer } = require('./util/testDrive')
const toPromise = require('./util/toPromise')

createSource().then(source => {
  var result = dt.RDD(source)

  function marshalTest (name, parts) {
    const op = parts.op
    const jsonOps = parts.jsonOps
    const verify = parts.verify
    // All operations run on the same source, the first marshal operations
    // should always be the parent definition.
    jsonOps.unshift({type: 'parent', key: source.key.toString('hex')})

    test(`marshal ${name}`, t => {
      t.same(op.marshal(), jsonOps, 'The marshal output fits expectations')
      t.end()
    })

    test(`unmarshal ${name}`, t =>
      createSparsePeer(source)
        .then(peer => toPromise(
          dt.unmarshal(
            peer,
            JSON.stringify(jsonOps)
          ).collect()
        ))
        .then(x => verify(t, x))
    )
  }

  test('making sure the source is correct', t =>
    toPromise(
      result
        .csv()
        .collect()
    )
    .then(x => {
      t.same(x.map(b => b.value), ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    })
  )

  marshalTest('csv', {
    op: result.csv(),
    jsonOps: [
      {type: 'csv', params: null, paramsType: undefined}
    ],
    verify: (t, x) => t.same(x.map(b => b.value), ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
  })

  marshalTest('splitBy', {
    op: result.splitBy(/\s/),
    jsonOps: [
      {type: 'splitBy', params: '\\s', paramsType: 'regexp'}
    ],
    verify: (t, x) => t.same(x, ['value', '1', '2', '3', '4', '5', '', 'value', '6', '7', '8', '9', '10', ''])
  })

  {
    const mapper = row => parseInt(row.value, 10) + 1
    marshalTest('map', {
      op: result.csv().map(mapper),
      jsonOps: [
        {type: 'csv', params: null, paramsType: undefined},
        {type: 'map', params: mapper.toString(), paramsType: undefined}
      ],
      verify: (t, x) => t.same(x, [2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    })
  }

  {
    const filter = x => x % 2 === 0
    const mapper = row => parseInt(row.value, 10)
    marshalTest('filter', {
      op: result
        .csv()
        .map(mapper)
        .filter(filter),
      jsonOps: [
        {type: 'csv', params: null, paramsType: undefined},
        {type: 'map', params: mapper.toString(), paramsType: undefined},
        {type: 'filter', params: filter.toString(), paramsType: undefined}
      ],
      verify: (t, x) => t.same(x, [2, 4, 6, 8, 10])
    })
  }

  marshalTest('get', {
    op: result.get('test.csv'),
    jsonOps: [
      {type: 'get', params: 'test.csv', paramsType: undefined}
    ],
    verify: (t, x) => t.same(x.toString(), 'value\n1\n2\n3\n4\n5\n')
  })

  {
    const selector = location => location === 'test.csv'
    marshalTest('select', {
      op: result.select(selector),
      jsonOps: [
        {type: 'select', params: selector.toString(), paramsType: undefined}
      ],
      verify: (t, x) => t.same(x.toString(), 'value\n1\n2\n3\n4\n5\n')
    })
  }
})
