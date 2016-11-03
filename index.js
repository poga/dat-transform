const through2 = require('through2')
const _ = require('highland')
const a = require('./action')
const tf = require('./transform')
const path = require('path')

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
  var next = this.transform(_.splitBy(sep))
  if (sep instanceof RegExp) {
    next._setMarshalInfo('splitBy', sep.source, 'regexp')
  } else {
    next._setMarshalInfo('splitBy', sep.source, 'string')
  }
  return next
}

RDD.prototype.map = function (f) {
  var next = this.transform(_.map(f))
  next._setMarshalInfo('map', f.toString())
  return next
}

RDD.prototype.filter = function (f) {
  var next = this.transform(_.filter(f))
  next._setMarshalInfo('filter', f.toString())
  return next
}

RDD.prototype.csv = function () {
  var next = this.transform(tf.csv())
  next._setMarshalInfo('csv', null)
  return next
}

RDD.prototype.sortBy = function (f) {
  var next = this.transform(_.sortBy(f))
  next._setMarshalInfo('sortBy', f.toString())
  return next
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

RDD.prototype.takeSortedBy = function (f) {
  return this.action(_.take(null), _.sortBy(f))
}

RDD.prototype.reduceByKey = function (f) {
  return this.action(_.take(null), a.reduceByKey(f))
}

RDD.prototype.count = function () {
  return this.action(counter(), sum())
}

RDD.prototype.sum = function () {
  return this.action(sum(), sum())
}

function counter (f) {
  return _.reduce(0, (sum, x) => sum + 1)
}

function sum () {
  return _.reduce(0, (sum, x) => sum + x)
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
    if (filter(entry) && !path.basename(entry.name).startsWith('.')) this.push(_(archive.createFileReadStream(entry)))
    cb()
  })))
}

RDD.prototype.marshal = function () {
  if (this._parent) {
    return this._parent.marshal().concat(this._marshalTransform())
  }

  return [{type: 'parent', key: this._archive.key.toString('hex')}]
}

RDD.prototype._marshalTransform = function () {
  return {
    type: this._marshalTransformType,
    params: this._marshalTransformParams,
    paramsType: this._marshalTransformParamsType
  }
}

RDD.prototype._setMarshalInfo = function (type, params, paramsType) {
  this._marshalTransformType = type
  this._marshalTransformParams = params
  this._marshalTransformParamsType = paramsType
}

function unmarshal (drive, json) {
  return _unmarshal(drive, null, JSON.parse(json))
}

/* eslint-disable no-eval */
function _unmarshal (drive, previous, transforms) {
  if (transforms.length === 0) return previous

  var head = transforms.shift()
  var rdd
  switch (head.type) {
    case 'parent':
      rdd = new RDD(drive.createArchive(head.key, {sparse: true}))
      break
    case 'splitBy':
      if (head.paramsType === 'regexp') {
        rdd = previous.splitBy(new RegExp(head.params))
      } else {
        rdd = previous.splitBy(head.params)
      }
      break
    case 'map':
      rdd = previous.map(eval(head.params))
      break
    case 'filter':
      rdd = previous.filter(eval(head.params))
      break
    case 'csv':
      rdd = previous.csv()
      break
    case 'sortBy':
      rdd = previous.sortBy(eval(head.params))
      break
  }

  return _unmarshal(drive, rdd, transforms)
}
/* eslint-enable no-new-func*/

// create key-value pairs for reduceByKey
function kv (k, v) {
  return {k: k, v: v}
}

module.exports = {RDD, kv, unmarshal}

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

