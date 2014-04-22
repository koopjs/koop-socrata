var should = require('should'),
  config = require('config'),
  koopserver = require('koop-server')(config);

global.config = config;

var data = require('./fixtures/earthquakes.json');

before(function (done) {
  global['Socrata'] = require('../models/Socrata.js');
  done();
});

describe('Socrata Model', function(){

    afterEach(function(done){
      done();
    });

    describe('socrata model methods', function() {
      before(function(done ){
        Cache.db = PostGIS.connect( config.db.postgis.conn );
        done();
      });
      it('toGeoJSON should err when given no data', function(done) {
        Socrata.toGeojson([], 'location', function(err, geojson){
          should.exist(err);
          should.not.exist( geojson );
          return done();
        });
      });

      it('toGeoJSON should return geojson', function(done) {
        Socrata.toGeojson(data, 'location', function(err, geojson){
          should.not.exist(err);
          should.exist( geojson );
          geojson.features.length.should.not.equal(0);
          return done();
        });
      });

      it('getResource should return geojson', function(done) {
        Socrata.getResource('https://data.seattle.gov', '2tje-83f6', {}, function(err, geojson){
          should.not.exist(err);
          should.exist( geojson );
          geojson[0].features.length.should.not.equal(0);
          return done();
        });
      });

    });

});




