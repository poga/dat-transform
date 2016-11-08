const _ = require('highland')

function reduceByKey (f) {
  return _.reduce({}, (sum, x) => {
    if (!sum[x[0]]) {
      sum[x[0]] = x[1]
    } else {
      sum[x[0]] = f(sum[x[0]], x[1])
    }
    return sum
  })
}

module.exports = {
  reduceByKey
}
