# dat-transform

Row-based, lazily-evaluated, transformation on [Dat](http://dat-data.com/) archives.
Similiar to [Spark](spark.apache.org)'s [Resilient Distributed Dataset(RDD)](https://amplab.cs.berkeley.edu/wp-content/uploads/2012/01/nsdi_spark.pdf)

## Usage

First, share your data with Dat:

```
$ cat names.txt
John
Alice
Jack
Stacy

$ dat .

```

#### Transform & Action

#### Select

#### Partition

## How it works

`dat-transform` use streams from [highland.js](http://highlandjs.org/), which provides lazy-evaluation and back-pressure.

## License

The MIT License
