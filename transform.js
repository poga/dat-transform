// action & transform is partial-applied highland function to enable lazy evaluation
const _ = require('highland')
const CSV = require('csv-parser')

// transforms
// file is a fileReadStream wrapped with highland
// every transform should return a function which returns a partial-applied highland function
function csv () {
  return file => _(file.pipe(CSV()))
}

module.exports = {csv}
