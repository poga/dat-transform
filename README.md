# dat-transform

Lazily-evaluated transformation on [Dat](http://dat-data.com/) archives.
Inspired by [Resilient Distributed Dataset(RDD)](https://amplab.cs.berkeley.edu/wp-content/uploads/2012/01/nsdi_spark.pdf)

[![Standard - JavaScript Style Guide](https://img.shields.io/badge/code%20style-standard-brightgreen.svg)](http://standardjs.com/)
[![npm](https://img.shields.io/npm/v/dat-transform.svg)]()

`npm i dat-transform`

## Synopsis

word count example:

```js
const {RDD, kv} = require('dat-transform')

const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
var drive = hyperdrive(memdb())
var archive = drive.createArchive(<DAT-ARCHIVE-KEY>, {sparse: true})

// define transforms
var wc = RDD(archive)
  .splitBy(/[\n\s]/)
  .filter(x => x !== '')
  .map(word => kv(word, 1))

// actual run(action)
wc
  .reduceByKey((x, y) => x + y)
  .toArray(res => {
    console.log(res) // [{bar: 2, baz: 1, foo: 1}]
  })
```

#### Transform & Action

**Transforms** are lazily-evaluated function on a dat archive.
Defining a transform on a RDD will not trigger computation immediately.
Instead, transformations will be pipelined and computed when we actually need the result, therefore provides opportunities of optimization.

**Transforms are applied to each file separately.**

Following transforms are included:

```
map(f)
filter(f)
splitBy(f)
sortBy(f) // check test/index.js for gotcha
```

**Actions** are operations that returns a value to the application.

Examples of actions:

```
collect()
take(n)
reduceByKey(f)
count()
sum()
takeSortedBy()
```

#### Select

`dat-transform` provides indexing via [hyperdrive](https://github.com/mafintosh/hyperdrive)'s list of entry.
You can specify the entries you want to computed with, which can greatly reduce bandwidth usage.

```
get(entryName)
select(f)
```

#### Partition

**Partitions** lets you re-index and cache the computed result to another archive.

```
partition(f, outArchive, cb)
```

#### Marshal/Unmarshal

Transforms can be marshalled as JSON. which allows execution on remote machine.

```
RDD.marshal
unmarshal
```

## How it works

`dat-transform` use streams from [highland.js](http://highlandjs.org/), which provides lazy-evaluation and back-pressure.

## License

The MIT License
