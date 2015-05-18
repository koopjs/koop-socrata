var pjson = require('./package.json')

exports.name = 'Socrata'
exports.hosts = true
exports.controller = require('./controller')
exports.routes = require('./routes')
exports.model = require('./models/Socrata.js')
exports.status = { version: pjson.version}
