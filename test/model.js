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
  host = 'https://data.seattle.gov',
  key = 'foobar',
  count = 1100

test('adding a socrata instance', function (t) {
  socrata.register(id, host, function (err, success) {
    if (err) throw err
    t.deepEqual(success, id)
    t.end()
  })
})

test('building pages', function (t) {
  t.plan(2)
  var pages = socrata.buildPages(host, key, count)
  t.deepEqual(pages[0], 'https://data.seattle.gov/resource/foobar.json?$order=:id&$limit=' + socrata.pageLimit + '&$offset=1')
  t.deepEqual(pages[1], 'https://data.seattle.gov/resource/foobar.json?$order=:id&$limit=' + socrata.pageLimit + '&$offset=1001')
})

test('parsing geojson', function (t) {
  t.plan(5)

  socrata.toGeojson([], 'location', [], function (err, geojson) {
    t.deepEqual(err, 'Error converting data to GeoJSON: JSON not returned from Socrata or blank JSON returned')
  })

  socrata.toGeojson(data, 'location', [], function (err, geojson) {
    if (err) throw err
    t.deepEqual(geojson.features.length, 1000)
  })

  var features = [{
    obj: {
      prop: true
    },
    location: {
      latitude: 0,
      longitude: 0
    }
  }, {
    location: {
      latitude: 0,
      longitude: 0
    }
  }]
  socrata.toGeojson(features, 'location', ['obj'], function (err, geojson) {
    if (err) throw err
    t.deepEqual(geojson.features[0].properties, {obj_prop: true})
    t.deepEqual(geojson.features[1].properties, {obj_prop: null})
    t.deepEqual(geojson.features.length, 2)
  })

})

test('stub the getFirst method', function (t) {
  sinon.stub(socrata, 'getFirst', function (host, id, callback) {
    callback(null, [{}])
  })

  sinon.stub(socrata, 'getRowCount', function (host, id, callback) {
    callback(null, 1)
  })

  sinon.stub(socrata, 'getMeta', function (host, id, callback) {
    var meta = {}
    meta.name = 'Test'
    meta.fields = []
    meta.location_field = null
    meta.updated_at = null
    callback(null, meta)
  })

  var feature = {
    type: 'Feature',
    properties: {},
    geometry: {
      type: 'Point',
      coordinates: [0, 0]
    }
  }

  sinon.stub(socrata, 'toGeojson', function (features, locationField, fields, callback) {
    callback(null, { features: [feature] })
  })
  sinon.stub(koop.Cache, 'insert', function (type, key, geojson, layer, callback) {
    callback(null, [{ features: [feature] }])
  })
  t.end()
})

// This test wont close
test('requesting data', function (t) {
  socrata.getResource(host, id, '2tje-83f68367', {}, function (err, geojson) {
    t.plan(5)
    if (err) throw err
    t.equal(socrata.getFirst.called, true)
    t.equal(socrata.getMeta.called, true)
    t.equal(socrata.getRowCount.called, true)
    t.equal(socrata.toGeojson.called, true)
    t.deepEqual(geojson[0].features.length, 1)
    t.end()
  })
})

test('teardown', function (t) {
  socrata.getFirst.restore()
  socrata.getMeta.restore()
  socrata.getRowCount.restore()
  socrata.toGeojson.restore()
  koop.Cache.insert.restore()
  t.end()
})
