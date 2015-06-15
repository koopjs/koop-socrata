var koop = require('koop')({})
var kooplib = require('koop/lib')
var sinon = require('sinon')
var test = require('tape')
var request = require('supertest')

// use Koop's local cache as a db for tests
kooplib.Cache = new kooplib.DataCache(koop)
kooplib.Cache.db = kooplib.LocalDB

var provider = require('../index.js')
var model = provider.model(kooplib)
var controller = provider.controller(model, kooplib.BaseController)

koop._bindRoutes(provider.routes, controller)

var sample_id = 'seattle'
var sample_host = 'https://data.seattle.gov'

// In the setup test we create several stubs that squash the
// normal behavoir of the model/controller methods.
// This allows us the only test the controller & routing and not the model here
test('setup', function (t) {
  sinon.stub(model, 'register', function (id, host, callback) {
    callback(null, id)
  })
  sinon.stub(model, 'find', function (id, callback) {
    callback(null, [{ 'id': sample_id, 'host': sample_host }])
  })
  sinon.stub(model, 'dropItem', function (host, item, options, callback) {
    callback(null, true)
  })
  sinon.stub(model, 'getResource', function (host, id, item, options, callback) {
    callback(null, {})
  })
  sinon.stub(model, 'getCount', function (key, options, callback) {
    callback(null, 0)
  })
  sinon.stub(controller, 'processFeatureServer', function (req, res, err, geojson, callback) {
    res.send({})
  })
  t.end()
})

test('register a socrata instance', function (t) {
  request(koop)
    .post('/socrata')
    .set('Content-Type', 'application/json')
    .send({
      'host': sample_host,
      'id': sample_id
    })
    .end(function () {
      t.equals(model.register.called, true)
      t.end()
    })
})

test('list the registered socrata instances', function (t) {
  request(koop)
    .get('/socrata')
    .set('Content-Type', 'application/json')
    .end(function () {
      t.equals(model.find.called, true)
      t.end()
    })
})

test('getting items calls the models find and findResource method', function (t) {
  request(koop)
    .get('/socrata/seattle/fake')
    .end(function () {
      t.equals(model.find.called, true)
      t.equals(model.getResource.called, true)
      t.end()
    })
})

test('dropping items calls the models dropItem method', function (t) {
  request(koop)
    .get('/socrata/seattle/fake/drop')
    .end(function () {
      t.equals(model.dropItem.called, true)
      t.end()
    })
})

test('getting a featureservice calls the model find, getResource methods and controllers featureservice method', function (t) {
  request(koop)
    .get('/socrata/seattle/fake/FeatureServer')
    .end(function () {
      t.equals(model.find.called, true)
      t.equals(model.getResource.called, true)
      t.equals(controller.processFeatureServer.called, true)
      t.end()
    })
})

test('getting a feature service count calls model get resource and get count', function (t) {
  request(koop)
    .get('/socrata/seattle/fake/FeatureServer/0/query?where=1=1&returnCountOnly=true')
    .end(function () {
      t.equals(model.getResource.called, true)
      t.equals(model.getCount.called, true)
      t.end()
    })
})

test('stub getResource and getCount for the following tests', function (t) {
  model.getResource.restore()
  model.getCount.restore()
  sinon.stub(model, 'getResource', function (host, id, item, options, callback) {
    callback(null, [{status: 'processing'}])
  })
  sinon.stub(model, 'getCount', function (key, options, callback) {
    callback('error', false)
  })
  t.end()
})

test('getting a resource that has never been requested before', function (t) {
  request(koop)
    .get('/socrata/seattle/fake')
    .end(function (err, res) {
      if (err) throw err
      t.deepEqual(res.body.status, 'processing')
      t.end()
    })
})

test('getting a resource that is processing does not fail', function (t) {
  request(koop)
    .get('/socrata/seattle/fake')
    .end(function (err, res) {
      if (err) throw err
      t.equals(model.getResource.called, true)
      t.equals(model.getCount.called, true)
      t.deepEqual(res.body.status, 'processing')
      t.end()
    })
})

test('teardown', function (t) {
  model.register.restore()
  model.find.restore()
  model.dropItem.restore()
  model.getResource.restore()
  model.getCount.restore()
  controller.processFeatureServer.restore()
  t.end()
})
