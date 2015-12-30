var koop = require('koop/lib')
var test = require('tape')
var fs = require('fs')
var nock = require('nock')
var sinon = require('sinon')
var JSONStream = require('JSONStream')
var es = require('event-stream')
var requests = nock('https://data.seattle.gov')

// responses for working resource
requests.get('/resource/foobar.json?$select=count(*)').times(2).reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes-count.json')))
requests.get('/resource/foobar.json?$order=:id&$limit=1').times(2).reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes-first.json')))
requests.get('/resource/foobar.json?$order=:id&$limit=1000&$offset=1').times(2).reply(200, function (uri) {return fs.createReadStream(__dirname + '/fixtures/crimes-page.json')})
requests.get('/views/foobar.json').times(2).reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes-views.json')), {'Last-Modified': 'Wed, 03 Jun 2015 10:05:45 PDT'})

// responses for non-existant resource
requests.get('/resource/missing.json?$select=count(*)').reply(404, JSON.parse(fs.readFileSync(__dirname + '/fixtures/missing-count.json')))
requests.get('/resource/missing.json?$order=:id&$limit=1').reply(404, JSON.parse(fs.readFileSync(__dirname + '/fixtures/missing-first.json')))
requests.get('/views/missing.json').reply(404, JSON.parse(fs.readFileSync(__dirname + '/fixtures/missing-views.json')))

// responses for resource where requesting the first row fails
requests.get('/resource/countFail.json?$select=count(*)').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/schools-count.json')))
requests.get('/resource/countFail.json?$order=:id&$limit=1').reply(500, JSON.parse(fs.readFileSync(__dirname + '/fixtures/schools-first.json')))
requests.get('/views/countFail.json').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/schools-view.json')))
requests.get('/resource/countFail.json').reply(200, function (uri) {return fs.createReadStream(__dirname + '/fixtures/schools-page.json')})

// responses for a resource that has been filered
requests.get('/resource/filtered.json?$select=count(*)').reply(500, JSON.parse(fs.readFileSync(__dirname + '/fixtures/filtered-count.json')))
requests.get('/resource/filtered.json?$order=:id&$limit=1').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/filtered-first.json')))
requests.get('/views/filtered.json').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/filtered-view.json')))
requests.get('/resource/filtered.json').reply(200, function (uri) {return fs.createReadStream(__dirname + '/fixtures/filtered-page.json')})

// responses for a zip file resource
requests.get('/resource/zip.json?$select=count(*)').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/zip-count.json')))
requests.get('/resource/zip.json?$order=:id&$limit=1').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/zip-first.json')))
requests.get('/views/zip.json').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/zip-view.json')))
requests.get('/api/geospatial/zip?method=export&format=Original').times(2).reply(200, function (uri) {return fs.createReadStream(__dirname + '/fixtures/zip-data.zip')})

// use Koop's local cache as a db for tests
koop.Cache = new koop.DataCache(koop)
koop.Cache.db = koop.LocalDB

koop.log = new koop.Logger({logfile: 'test'})

var socrata = require('../models/Socrata.js')(koop)

socrata.pageLimit = 1000

var data = JSON.parse(fs.readFileSync(__dirname + '/fixtures/earthquakes.json'))
var id = 'seattle'
var host = 'https://data.seattle.gov'
var key = 'foobar'

// stub out requests for a zip resource
sinon.stub(socrata, 'ogrZip', function (stream, callback) {
  callback(null, JSON.parse(fs.readFileSync(__dirname + '/fixtures/zip-geojson.json')))
})

test('adding a socrata instance', function (t) {
  socrata.register(id, host, function (err, success) {
    if (err) throw err
    t.deepEqual(success, id)
    t.end()
  })
})

test('building pages', function (t) {
  t.plan(2)
  var pages = socrata.buildPages(host, 'happyPath', 2001)
  t.deepEqual(pages[0], 'https://data.seattle.gov/resource/happyPath.json?$order=:id&$limit=' + socrata.pageLimit + '&$offset=1')
  t.deepEqual(pages[1], 'https://data.seattle.gov/resource/happyPath.json?$order=:id&$limit=' + socrata.pageLimit + '&$offset=' + (1 + socrata.pageLimit))
})

test('getting the first row', function (t) {
  t.plan(1)
  socrata.getFirst(host, key, function (err, data) {
    if (err) throw err
    t.deepEqual(data, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes-first.json')))
  })
})

test('getting the row count', function (t) {
  t.plan(1)
  socrata.getRowCount(host, key, function (err, count) {
    if (err) throw err
    t.deepEqual(count, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes-count.json'))[0].count)
  })
})

test('getting the metadata', function (t) {
  t.plan(4)
  socrata.getMeta(host, key, function (err, meta) {
    if (err) throw err
    t.deepEqual(meta.updated_at, new Date('Wed, 03 Jun 2015 10:05:45 PDT'))
    t.deepEqual(meta.location_field, 'incident_location')
    t.deepEqual(meta.name, 'Seattle Police Department 911 Incident Response')
    t.deepEqual(meta.fields, [
      'cad_cdw_id',
      'cad_event_number',
      'general_offense_number',
      'event_clearance_code',
      'event_clearance_description',
      'event_clearance_subgroup',
      'event_clearance_group',
      'event_clearance_date',
      'hundred_block_location',
      'district_sector',
      'zone_beat',
      'census_tract',
      'longitude',
      'latitude',
      'incident_location',
      'initial_type_description',
      'initial_type_subgroup',
      'initial_type_group',
      'at_scene_time' ])
  })
})

test('getting a full page', function (t) {
  t.plan(1)
  var url = 'https://data.seattle.gov/resource/foobar.json?$order=:id&$limit=1000&$offset=1'
  var json = []
  socrata.getPage(url)
    .pipe(JSONStream.parse('*'))
    .pipe(es.map(function (data, callback) {
      json.push(data)
      callback()
    }))
    .on('end', function () {
      t.deepEqual(json, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes-page.json')))
    })
})

test('parsing geojson', function (t) {
  t.plan(5)

  var meta = {location_field: 'location', fields: [], field_types: []}

  socrata.toGeojson([], meta, function (err, geojson) {
    t.deepEqual(err, 'Error converting data to GeoJSON: JSON not returned from Socrata or blank JSON returned')
  })

  socrata.toGeojson(data, meta, function (err, geojson) {
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

  meta.fields = ['obj']

  socrata.toGeojson(features, meta, function (err, geojson) {
    if (err) throw err
    t.deepEqual(geojson.features[0].properties, {obj: null, obj_prop: true})
    t.deepEqual(geojson.features[1].properties, {obj: null, obj_prop: null})
    t.deepEqual(geojson.features.length, 2)
  })
})

test('piping data through the process stream', function (t) {
  t.plan(2)
  sinon.stub(socrata, 'getPage', function (url) {
    return fs.createReadStream(__dirname + '/fixtures/crimes-page.json')
  })

  sinon.stub(socrata, 'toGeojson', function (json, meta, callback) {
    callback(null, { type: 'FeatureCollection', features: [{}] })
  })
  var meta = {}
  socrata.processStream(socrata.getPage('foo'), meta, function (err, geojson) {
    if (err) throw err
    t.deepEqual(socrata.toGeojson.called, true)
    t.deepEqual(geojson.features.length, 1000)
    socrata.toGeojson.restore()
    socrata.getPage.restore()
  })
})

test('stubbing methods for cache check', function (t) {
  sinon.stub(socrata, 'getMeta', function (host, id, callback) {
    callback(null, {updated_at: new Date()})
  })
  sinon.stub(socrata, 'dropItem', function (host, id, layer, callback) {
    callback(null, null)
  })
  sinon.stub(socrata, 'getResource', function (host, id, layer, options, callback) {
    callback(null, null)
  })
  sinon.stub(koop.Cache, 'getInfo', function (table, callback) {
    var info = {
      updated_at: new Date('Thu Jun 03 2015').toString(),
      checked_at: new Date('Thu Jun 03 2015').toString()
    }
    callback(null, info)
  })
  t.end()
})

test('correctly dropping and rebuilding an expired item', function (t) {
  t.plan(3)
  socrata.checkExpiration(host, key, function (err, expired) {
    if (err) throw err
    t.deepEqual(socrata.dropItem.called, true)
    t.deepEqual(socrata.getResource.called, true)
    t.deepEqual(expired, true)
    koop.Cache.getInfo.restore()
    socrata.getMeta.restore()
    socrata.getResource.restore()
    socrata.dropItem.restore()
  })
})

test('stubbing method for non expired cache check', function (t) {
  sinon.stub(socrata, 'getMeta', function (host, id, callback) {
    callback(null, {updated_at: new Date()})
  })
  sinon.stub(socrata, 'dropItem', function (host, id, layer, callback) {
    callback(null, null)
  })
  sinon.stub(socrata, 'getResource', function (host, id, layer, options, callback) {
    callback(null, null)
  })
  sinon.stub(koop.Cache, 'getInfo', function (table, callback) {
    var info = {
      updated_at: new Date().toString(),
      checked_at: new Date().toString()
    }
    callback(null, info)
  })
  t.end()
})

test('not dropping a non expired cache', function (t) {
  t.plan(3)
  socrata.checkExpiration(host, key, function (err, expired) {
    if (err) throw err
    t.deepEqual(socrata.dropItem.called, false)
    t.deepEqual(socrata.getResource.called, false)
    t.deepEqual(expired, false)
    koop.Cache.getInfo.restore()
    socrata.getMeta.restore()
    socrata.getResource.restore()
    socrata.dropItem.restore()
  })
})

test('fill the cache with an errored dataset', function (t) {
  socrata.getResource(host, id, 'missing', {}, function (err, data) {
    if (err) throw err
    setTimeout(function () {
      t.deepEqual((typeof koop.Cache.db.store['Socrata:missing:0']).toString(), 'object')
      t.end()
    }, 500)
  })
})

// this test will hang the whole process and never exit so call process.exit(0) in the teardown
test('requesting a socrata dataset that does not exist', function (t) {
  t.plan(2)
  socrata.getResource(host, id, 'missing', {}, function (err, info) {
    if (err) throw err
    t.deepEqual(info[0].status, 'processing')
    t.deepEqual(info[0].errors.length, 3)
    t.end()
  })
})

test('processing a zip file', function (t) {
  t.plan(1)
  socrata.processZip(host, 'zip', function (err, geojson) {
    if (err) throw err
    t.deepEqual(geojson.features.length, 19)
  })
})

// Integration tests

test('fill the cache with a resource that is a zip', function (t) {
  socrata.getResource(host, id, 'zip', {}, function (err, data) {
    if (err) throw err
    setTimeout(function () {
      t.end()
    }, 500)
  })
})

test('requesting a resource that was a zip', function (t) {
  t.plan(1)
  socrata.getResource(host, id, 'zip', {layer: 0}, function (err, data) {
    if (err) throw err
    t.deepEqual(data[0].features.length, 19)
  })
})

test('fill the cache with a resource that was filtered', function (t) {
  socrata.getResource(host, id, 'filtered', {}, function (err, data) {
    if (err) throw err
    setTimeout(function () {
      t.end()
    }, 500)
  })
})

test('requesting a resource that was filtered', function (t) {
  t.plan(1)
  socrata.getResource(host, id, 'filtered', {layer: 0}, function (err, data) {
    if (err) throw err
    t.deepEqual(data[0].features.length, 82)
  })
})

test('fill the cache with a dataset that fails on the first row count', function (t) {
  socrata.getResource(host, id, 'countFail', {}, function (err, data) {
    if (err) throw err
    setTimeout(function () {
      t.end()
    }, 500)
  })
})

test('requesting a resource where the first row request fails', function (t) {
  t.plan(1)
  var geojson = JSON.parse(fs.readFileSync(__dirname + '/fixtures/schools-geojson.json'))
  socrata.getResource(host, id, 'countFail', {layer: 0}, function (err, data) {
    if (err) throw err
    t.deepEqual(data[0].features.length, geojson.features.length)
  })
})

test('fill the cache with a fully working resource', function (t) {
  sinon.stub(socrata, 'checkExpiration', function (host, id, callback) {
    callback(null, false)
  })
  socrata.getResource(host, id, 'foobar', {layer: 0}, function (err, data) {
    if (err) throw err
    setTimeout(function () {
      t.deepEqual(socrata.checkExpiration.called, false)
      t.deepEqual(typeof koop.Cache.db.store['Socrata:foobar:0'], 'object')
      t.end()
    }, 1000)
  })
})

test('requesting a resource with a fully working resource', function (t) {
  t.plan(1)
  socrata.getResource(host, id, 'foobar', {layer: 0}, function (err, data) {
    if (err) throw err
    t.deepEqual(data[0].features.length, 1001)
  })
  socrata.checkExpiration.restore()
})

test('teardown', function (t) {
  socrata.ogrZip.restore()
  t.end()
})
