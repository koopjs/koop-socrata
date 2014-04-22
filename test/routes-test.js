var should = require('should'),
  request = require('supertest'),
  config = require('config'),
  koop = require('koop-server')(config);

global.config = config;

var resource = 'f7f2-ggz5';

before(function (done) {
    Cache.db = PostGIS.connect( config.db.postgis.conn );
    try { koop.register(require("../index.js")); } catch(e){}
    done();
});

describe('Koop Routes', function(){

    before(function(done){
      request(koop)
          .post('/socrata/register')
          .set('Content-Type', 'application/json')
          .send({ 
            'host': 'https://data.cityofchicago.org', 
            'id': 'tester'
          })
          .end(function(err, res){
            res.should.have.status(200);
            done();
      });
    });

    after(function(done){
      request(koop)
          .del('/socrata/tester')
          .end(function(err, res){
            res.should.have.status(200);
            done();
      });
    });


    describe('/socrata routes', function() {
      it('register should return 500 when POSTing w/o a host', function(done) {
        request(koop)
          .post('/socrata/register')
          .end(function(err, res){
            res.should.have.status(500);
            done();
        });
      });

      it('should return 200 when GETing all registered providers', function(done) {
          request(koop)
            .get('/socrata')
            .end(function(err, res){
              res.should.have.status( 200 );
              done();
          });
      });

      it('should return 200 when GETing a registered provider', function(done) {
          request(koop)
            .get('/socrata/tester')
            .end(function(err, res){
              res.should.have.status( 200 );
              done();
          });
      });

      it('should return 404 when GETing an unknown provider/host', function(done) {
          request(koop)
            .get('/socrata/bogus')
            .end(function(err, res){
              res.should.have.status( 404 );
              done();
          });
      });

      it('should return 200 when accessing item data', function(done) {
          request(koop)
            .get('/socrata/tester/' + resource )
            .end(function(err, res){
              res.should.have.status( 200 );
              should.not.exist(err);
              done();
          });
      });

      it('should return 200 when accessing item as a featureservice', function(done) {
          request(koop)
            .get('/socrata/tester/' + resource + '/FeatureServer')
            .end(function(err, res){
              res.should.have.status( 200 );
              should.not.exist(err);
              done();
          });
      });

      it('should return 200 when accessing item as a featureservice layer', function(done) {
          request(koop)
            .get('/socrata/tester/' + resource + '/FeatureServer/0')
            .end(function(err, res){
              res.should.have.status( 200 );
              should.not.exist(err);
              done();
          });
      });

      it('should return 200 when accessing item as a featureservice query', function(done) {
          request(koop)
            .get('/socrata/tester/' + resource + '/FeatureServer/0/query')
            .end(function(err, res){
              res.should.have.status( 200 );
              should.not.exist(err);
              done();
          });
      });

    });

});
