var extend = require('node.extend'),
  var sm = require('sphericalmercator'),
  merc = new sm({size:256}),
  fs = require('fs'),
  crypto = require('crypto');

// inherit from base controller
var Controller = extend({
  serviceName: 'socrata',

  register: function(req, res){
    if ( !req.body.host ){
      res.send('Must provide a host to register:', 500); 
    } else { 
      Socrata.register( req.body.id, req.body.host, function(err, id){
        if (err) {
          res.send( err, 500);
        } else {
          res.json({ 'serviceId': id });
        }
    });
    }
  },

  list: function(req, res){
    Socrata.find(null, function(err, data){
      if (err) {
        res.send( err, 500);
      } else {
        res.json( data );
      }
    });
  }, 

  find: function(req, res){
    Socrata.find(req.params.id, function(err, data){
      if (err) {
        res.send( err, 404);
      } else {
        res.json( data );
      }
    });
  },

  findResourcePost: function( req, res ){
    Controller.findResource( req, res );
  },

  findResource: function(req, res){
    Socrata.find(req.params.id, function(err, data){
      if (err) {
        res.send( err, 500);
      } else {
        // Get the item 
        Socrata.getResource( data.host, req.params.item, req.query, function(error, itemJson){
          if (error) {
            res.send( error, 500);
          } else if ( req.params.format ) {
            // change geojson to json
            req.params.format = req.params.format.replace('geojson', 'json');

            var dir = ['socrata', req.params.id ].join(':');
            // build the file key as an MD5 hash that's a join on the paams and look for the file 
            var toHash = JSON.stringify( req.params ) + JSON.stringify( req.query );
            var key = crypto.createHash('md5').update( toHash ).digest('hex');

            var fileName = [sails.config.data_dir + 'files', dir, key + '.' + req.params.format].join('/');

            if (fs.existsSync( fileName )){
              res.sendfile( fileName );
            } else {
              Exporter.exportToFormat( req.params.format, key, key, itemJson[0], {}, function(err, file){
                if (err){
                  res.send(err, 500);
                } else {
                  res.sendfile( file );
                }
              });
            }
          } else { 
            res.json( itemJson[0] );
          }
        });
      }
    });
  },

  del: function(req, res){
    if ( !req.params.id ){
      res.send( 'Must specify a service id', 500 );
    } else { 
      Socrata.remove(req.params.id, function(err, data){
        if (err) {
          res.send( err, 500);
        } else {
          res.json( data );
        }
      });
    }
  }, 
  
  featureserver: function( req, res ){
    var callback = req.query.callback;
    delete req.query.callback;
    
    for (var k in req.body){
      req.query[k] = req.body[k];
    }

    Socrata.find(req.params.id, function(err, data){
      if (err) {
        res.send( err, 500);
      } else {
        // Get the item 
        Socrata.getResource( data.host, req.params.item, req.query, function(error, geojson){
          if (error) {
            res.send( error, 500);
          } else {
            // pass to the shared logic for FeatureService routing
            delete req.query.geometry;
            Controller._processFeatureServer( req, res, err, geojson, callback);
          }
        });
      }
    });
    
  },

  tiles: function( req, res ){
    var callback = req.query.callback;
    delete req.query.callback;

    var key,
      layer = req.params.layer || 0;

    var _send = function( err, data ){
        req.params.key = key + ':' + layer;
        if (req.query.style){
          req.params.style = req.query.style;
        }
        Tiles.get( req.params, data[ layer ], function(err, tile){
          if ( req.params.format == 'png' || req.params.format == 'pbf'){
            res.sendfile( tile );
          } else {
            if ( callback ){
              res.send( callback + '(' + JSON.stringify( tile ) + ')' );
            } else {
              if (typeof tile == 'string'){
                res.sendfile( tile );
              } else {
                res.json( tile );
              }
            }
          }
        });
    }

    // build the geometry from z,x,y
    var bounds = merc.bbox( req.params.x, req.params.y, req.params.z );

    req.query.geometry = {
        xmin: bounds[0],
        ymin: bounds[1],
        xmax: bounds[2],
        ymax: bounds[3],
        spatialReference: { wkid: 4326 }
    };

    var _sendImmediate = function( file ){
      if ( req.params.format == 'png' || req.params.format == 'pbf'){
        res.sendfile( file );
      } else {
        fs.readFile(file, function(err, data){
          if ( callback ){
            res.send( callback + '(' + JSON.parse(data) + ')' );
          } else {
            res.json( JSON.parse(data) );
          }
        })
      }
    };

    var key = ['socrata', req.params.id, req.params.item].join(':');
    var file = config.data_dir + '/tiles/';
      file += key + '/' + req.params.format;
      file += '/' + req.params.z + '/' + req.params.x + '/' + req.params.y + '.' + req.params.format;

    var jsonFile = file.replace(/png|pbf|utf/g, 'json');

    // if the json file alreadty exists, dont hit the db, just send the data
    if (fs.existsSync(jsonFile) && !fs.existsSync( file ) ){
      _send( null, fs.readFileSync( jsonFile ) );
    } else if ( !fs.existsSync( file ) ) {
      Socrata.find(req.params.id, function(err, data){
        if (err) {
          res.send( err, 500);
        } else {
          // Get the item 
          Socrata.getResource( data.host, req.params.item, req.query, _send );
        }
      });
    } else {
      _sendImmediate(file);
    }

  },


  thumbnail: function(req, res){

    // check the image first and return if exists
    var key = ['socrata', req.params.id, req.params.item].join(':');
    var dir = sails.config.data_dir + '/thumbs/';
    req.query.width = parseInt( req.query.width ) || 150;
    req.query.height = parseInt( req.query.height ) || 150;
    req.query.f_base = dir + key + '/' + req.query.width + '::' + req.query.height;

    var fileName = Thumbnail.exists(key, req.query);
    if ( fileName ){
      res.sendfile( fileName );
    } else {

      Socrata.find(req.params.id, function(err, data){
        if (err) {
          res.send( err, 500);
        } else {
          // Get the item 
          Socrata.getResource( data.host, req.params.item, req.query, function(error, itemJson){
            if (error) {
              res.send( error, 500);
            } else {
              var key = ['socrata', req.params.id, req.params.item].join(':');

              // generate a thumbnail
              Thumbnail.generate( itemJson[0], key, req.query, function(err, file){
                if (err){
                  res.send(err, 500);
                } else {
                  // send back image
                  res.sendfile( file );
                }
              });

            }
          });
        }
      });
    }

  },

  
  preview: function(req, res){
   res.render(__dirname + '/../views/demo', { locals:{ host: req.params.id, item: req.params.item } });
  }



}, BaseController);

module.exports = Controller;
