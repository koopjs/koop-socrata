var koop = require('koop/lib'),
  test = require('tape'),
  sinon = require('sinon'),
  fs = require('fs')

// use Koop's local cache as a db for tests
koop.Cache = new koop.DataCache(koop)
koop.Cache.db = koop.LocalDB

var socrata = require('../models/Socrata.js')(koop)
var data = JSON.parse(fs.readFileSync(__dirname + '/fixtures/earthquakes.json'))

var id = 'seattle',
  host = 'https://data.seattle.gov'

test('adding a socrata instance', function (t) {
  socrata.register(id, host, function (err, success) {
    if (err) throw err
    t.deepEqual(success, id)
    t.end()
  })
})

test('parsing geojson', function (t) {
  t.plan(2)

  socrata.toGeojson([], 'location', function (err, geojson) {
    t.deepEqual(err, 'Error converting data to geojson')
  })

  socrata.toGeojson(data, 'location', function (err, geojson) {
    if (err) throw err
    t.deepEqual(geojson.features.length, 1000)
  })

})

test('stub the request method', function (t) {
  sinon.stub(socrata, 'request', function (url, callback) {
    callback(null, {
      'body': '{ "features": [], "name": "Test" }',
      'headers': {
        'x-soda2-types': '[]',
        'x-soda2-fields': '[]'
      }
    })
  })

  var feature = {
    type: 'Feature',
    properties: {},
    geometry: {
      type: 'Point',
      coordinates: [0, 0]
    }
  }

  sinon.stub(socrata, 'toGeojson', function (features, locationField, callback) {
    callback(null, { features: [feature] })
  })
  sinon.stub(koop.Cache, 'insert', function (type, key, geojson, layer, callback) {
    callback(null, [{ features: [feature] }])
  })
  t.end()
})

// This test wont close
test('requesting data', function (t) {
  socrata.getResource(host, id, '2tje-83f6', {}, function (err, geojson) {
    if (err) throw err
    t.equal(socrata.request.called, true)
    t.equal(socrata.toGeojson.called, true)
    t.deepEqual(geojson[0].features.length, 1)
    t.end()
  })
})

test('teardown', function (t) {
  socrata.request.restore()
  socrata.toGeojson.restore()
  koop.Cache.insert.restore()
  t.end()
})
