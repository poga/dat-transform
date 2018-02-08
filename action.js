'use strict'
const _ = require('highland')

exports.reduceByKey = function (f) {
  return _.reduce({}, (sum, x) => {
    if (!sum[x[0]]) {
      sum[x[0]] = x[1]
    } else {
      sum[x[0]] = f(sum[x[0]], x[1])
    }
    return sum
  })
}
