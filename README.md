# dat-transform

Lazily-evaluated transformation on [Dat](http://dat-data.com/) archives.
Similiar to Spark's [Resilient Distributed Dataset(RDD)](https://amplab.cs.berkeley.edu/wp-content/uploads/2012/01/nsdi_spark.pdf)

## Usage

word count example:

```js
const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const dt = require('dat-transform')

var drive = hyperdrive(memdb())
var archive = drive.createArchive(<DAT-ARCHIVE-KEY>)

// define transforms
var result = dt.RDD(archive)
  .splitBy(/[\n\s]/)
  .filter(x => x !== '')
  .map(word => dt.kv(word, 1))

// actual run(action)
result.reduceByKey((x, y) => x + y)
  .toArray(res => {
    console.log(res) // [{bar: 2, baz: 1, foo: 1}]
  })
```

#### Transform & Action

**Transforms** are lazily-evaluated function on a dat archive.
Define a transform on a RDD will not trigger computation immediately.
Instead, transformations will be pipelined and computed when we actually need the result, therefore provides opportunities of optimization.

Following transforms are included:

```
map(f)
filter(f)
splitBy(f)
```

**Actions** are operations that returns a value to the application.

Examples of actions:

```
collect()
take(n)
reduceByKey(f)
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

## How it works

`dat-transform` use streams from [highland.js](http://highlandjs.org/), which provides lazy-evaluation and back-pressure.

## License

The MIT License
