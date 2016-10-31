const through2 = require('through2')
const _ = require('highland')
const a = require('./action')
const tf = require('./transform')

function RDD (archive, parent, transform) {
  if (!(this instanceof RDD)) return new RDD(archive, parent, transform)

  this._archive = archive
  this._parent = parent
  this._transform = transform // transform is a stream, which will be applied to each file
  this._selector = all
}

RDD.prototype.partition = function (f, outArchive) {
  var partitions = {}

  return new Promise((resolve, reject) => {
    this
      .action(_.take(null), _.map(x => { return {p: f(x), v: x} }))
      .each(x => getPartition(x.p).write(`${x.v}`))
      .done(endPartitions)

    function endPartitions () {
      Object.keys(partitions).forEach(k => partitions[k].end())
      outArchive.finalize(() => { resolve(new RDD(outArchive)) })
    }
  })

  function getPartition (key) {
    if (!partitions[key]) {
      partitions[key] = _.pipeline(
        _.intersperse('\n'),
        outArchive.createFileWriteStream(`${key}`)
      )
    }
    return partitions[key]
  }
}

RDD.prototype.get = function (filename) {
  if (this._transform) throw new Error('Cannot get file after transformation')

  this._selector = x => x.name === filename
  return this
}

RDD.prototype.select = function (selector) {
  if (this._transform) throw new Error('Cannot select files after transformation')

  this._selector = selector
  return this
}

// define a lazily-evaluated transformation on this RDD, retruns new RDD
// ususally, you want to use transform defined below instead of use this method
RDD.prototype.transform = function (transform) {
  return new RDD(this._archive, this, transform)
}

// transforms
RDD.prototype.splitBy = function (sep) {
  return this.transform(_.splitBy(sep))
}

RDD.prototype.map = function (f) {
  return this.transform(_.map(f))
}

RDD.prototype.filter = function (f) {
  return this.transform(_.filter(f))
}

RDD.prototype.csv = function () {
  return this.transform(tf.csv())
}

// do action
// evaluation transformation chain. Then apply a transform to each file, then apply a transform to flattened data
// usually, you want to use actions defined below instead of use this method
RDD.prototype.action = function (fileAction, totalAction) {
  return this._applyTransform()
    .pipe(mapToFilePipe(fileAction))
    .flatten()
    .pipe(pipe(totalAction))
}

// actions
RDD.prototype.collect = function () {
  return this.take(null)
}

RDD.prototype.take = function (n) {
  return this.action(_.take(n), _.take(n))
}

RDD.prototype.reduceByKey = function (f) {
  return this.action(_.take(null), a.reduceByKey(f))
}

// private methods
//
// build a transformation chain without evaluating it
RDD.prototype._applyTransform = function () {
  if (this._parent) {
    return mapToFile(this._transform)(this._parent._applyTransform())
  }

  return this._eachFile(this._selector)
}

// eachFile returns a stream of stream, each inner stream is a fileReadStream
// all stream is wrapped with highland
RDD.prototype._eachFile = function (filter) {
  var archive = this._archive
  return _(archive.list({live: false}).pipe(through2.obj(function (entry, enc, cb) {
    if (filter(entry)) this.push(_(archive.createFileReadStream(entry)))
    cb()
  })))
}

// create key-value pairs for reduceByKey
function kv (k, v) {
  return {k: k, v: v}
}

module.exports = {RDD, kv}

function mapToFilePipe (action) {
  return pipe(_.map(file => action(file)))
}

function pipe (action) {
  return _.pipeline(action)
}

function mapToFile (transform) {
  return _.map(file => transform(file))
}

function all (x) {
  return true
}

