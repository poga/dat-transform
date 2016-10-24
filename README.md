# dat-transform

Lazily-evaluated transformation on [Dat](http://dat-data.com/) archives.
Similiar to [Spark](http://spark.apache.org)'s [Resilient Distributed Dataset(RDD)](https://amplab.cs.berkeley.edu/wp-content/uploads/2012/01/nsdi_spark.pdf)

## Usage

word count example:

```js
const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const dt = require('dat-transform')
var drive = hyperdrive(memdb())

// create a new hyperspark RDD point to a existing dat archive
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

#### Select

#### Partition

## How it works

`dat-transform` use streams from [highland.js](http://highlandjs.org/), which provides lazy-evaluation and back-pressure.

## License

The MIT License
