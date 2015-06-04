var koop = require('koop/lib'),
  test = require('tape'),
  fs = require('fs'),
  nock = require('nock'),
  JSONStream = require('JSONStream'),
  es = require('event-stream')

var requests = nock('https://data.seattle.gov')

// responses for working resource
requests.get('/resource/foobar.json?$select=count(*)').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes::count.json')))
requests.get('/resource/foobar.json?$order=:id&$limit=1').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes::first.json')))
requests.get('/resource/foobar.json?$order=:id&$limit=1000&$offset=1').reply(200, function (uri) {return fs.createReadStream(__dirname + '/fixtures/crimes::page.json')})
requests.get('/views/foobar.json').reply(200, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes::views.json')), {'Last-Modified': 'Wed, 03 Jun 2015 10:05:45 PDT'})

// responses for non-exist resource
requests.get('/resource/missing.json?$select=count(*)').reply(404, JSON.parse(fs.readFileSync(__dirname + '/fixtures/missing::count.json')))
requests.get('/resource/missing.json?$order=:id&$limit=1').reply(404, JSON.parse(fs.readFileSync(__dirname + '/fixtures/missing::first.json')))
requests.get('/views/missing.json').reply(404, JSON.parse(fs.readFileSync(__dirname + '/fixtures/missing::views.json')))

// use Koop's local cache as a db for tests
koop.Cache = new koop.DataCache(koop)
koop.Cache.db = koop.LocalDB
koop.log = new koop.Logger({logfile: 'test_log.log'})

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

test('getting the first row', function (t) {
  t.plan(1)
  socrata.getFirst(host, key, function (err, data) {
    if (err) throw err
    t.deepEqual(data, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes::first.json')))
  })
})

test('getting the row count', function (t) {
  t.plan(1)
  socrata.getRowCount(host, key, function (err, count) {
    if (err) throw err
    t.deepEqual(count, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes::count.json'))[0].count)
  })
})

test('getting the metadata', function (t) {
  t.plan(4)
  socrata.getMeta(host, key, function (err, meta) {
    if (err) throw err
    t.deepEqual(meta.updated_at, new Date('Wed, 03 Jun 2015 10:05:45 PDT').getTime())
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
    t.deepEqual(json, JSON.parse(fs.readFileSync(__dirname + '/fixtures/crimes::page.json')))
  })
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

var resource = 'missing'
test('fill the cache with an errored dataset', function (t) {
  socrata.getResource(host, id, resource, {}, function (err, data) {
    if (err) throw err
    setTimeout(function () {
      t.end()
    }, 10)
  })
})

// this test will hang the whole process and never exit so call process.exit(0) in the teardown
test('requesting a socrata dataset that does not exist', function (t) {
  t.plan(2)
  socrata.getResource(host, id, resource, {}, function (err, info) {
    if (err) throw err
    t.deepEqual(info.status, 'processing')
    t.deepEqual(info.errors.length, 3)
    t.end()
  })
})

test('teardown', function (t) {
  t.pass()
  t.end()
  process.exit(0)
})
