var should = require('should'),
  config = require('config'),
  koop = require('koop-server/lib');

var data = require('./fixtures/earthquakes.json');

before(function(done){
  // setup koop 
  koop.Cache.db = koop.PostGIS.connect( config.db.postgis.conn );
  var data_dir = __dirname + '/output/';
  koop.Cache.data_dir = data_dir;
  Socrata = new require('../models/Socrata.js')( koop );
  done();
});

describe('Socrata Model', function(){

    afterEach(function(done){
      done();
    });

    describe('socrata model methods', function() {
      before(function(done ){
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




