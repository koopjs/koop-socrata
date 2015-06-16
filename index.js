var pkg = require('./package.json')

var provider = {
  name: 'Socrata',
  hosts: true,
  controller: require('./controller'),
  routes: require('./routes'),
  model: require('./models/Socrata'),
  status: {
    version: pkg.version
  }
}

module.exports = provider
