'use strict'
const Hyperdrive = require('hyperdrive')
const ram = require('random-access-memory')
const fs = require('fs')
const path = require('path')
const bb = require('bluebird')

function streamToPromise (stream) {
  return new Promise((resolve, reject) => {
    stream.on('error', reject)
    stream.on('finish', resolve)
  })
}

function replicate (inDrive, outDrive) {
  var stream = inDrive.replicate()
  stream.pipe(outDrive.replicate()).pipe(stream)
}

function foldersForFiles (files) {
  const folders = []
  const map = {}
  Object.keys(files).forEach(file => {
    let folder = path.dirname(file)
    while (folder !== '.' && folder !== '/') {
      if (!map[folder]) {
        folders.push(folder)
      }
      folder = path.dirname(folder)
    }
  })
  return folders.sort()
}

function createDrive (key, opts) {
  return new Hyperdrive(ram, key, opts)
}

function createSource (files) {
  if (!files) {
    files = {
      'test.csv': null,
      'test2.csv': null
    }
  }
  const source = createDrive()
  const folders = foldersForFiles(files)

  return bb
    .mapSeries(folders, folder => new Promise((resolve, reject) => {
      source.mkdir(folder, err => err ? reject(err) : resolve())
    }))
    .then(() => bb.map(Object.keys(files), file => {
      const inFile = path.join(__dirname, '..', files[file] || file)
      const inStream = fs.createReadStream(inFile)
      // console.log(inFile, '->', file)
      const outStream = source.createWriteStream(file)
      return streamToPromise(inStream.pipe(outStream))
    }))
    .then(res => source)
}

function createSparsePeer (source, opts) {
  return (source ? Promise.resolve(source) : createSource(opts ? opts.files : null))
    .then(source => {
      const peer = createDrive(source.key, {sparse: true})
      if (opts && opts.storeProgress) {
        peer.downloaded = NaN
        peer.on('content', () => {
          peer.downloaded = 0
          peer.content.on('download', (index, data, from) => {
            peer.downloaded += data.length
          })
        })
      }
      replicate(source, peer)
      return new Promise(resolve => {
        peer.on('ready', () => resolve(peer))
      })
    })
}

module.exports = {
  createDrive,
  createSource,
  createSparsePeer
}
