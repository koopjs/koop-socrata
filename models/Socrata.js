var request = require('request'),
  BaseModel = require('koop-server/lib/BaseModel.js');

var Socrata = function( koop ){

  var socrata = {};
  socrata.__proto__ = BaseModel( koop );

  // adds a service to the koop.Cache.db
  // needs a host, generates an id 
  socrata.register = function( id, host, callback ){
    var type = 'socrata:services';
    koop.Cache.db.serviceCount( type, function(error, count){
      id = id || count++;
      koop.Cache.db.serviceRegister( type, {'id': id, 'host': host},  function( err, success ){
        callback( err, id );
      });
    });
  };

  socrata.remove = function( id, callback ){
    koop.Cache.db.serviceRemove( 'socrata:services', parseInt(id) || id,  callback);
  }; 

  // get service by id, no id == return all
  socrata.find = function( id, callback ){
    koop.Cache.db.serviceGet( 'socrata:services', parseInt(id) || id, callback);
  };

  socrata.socrata_path = '/resource/';
  socrata.socrata_view_path = '/resource/';

  // got the service and get the item
  socrata.getResource = function( host, id, options, callback ){
    var self = this,
      type = 'Socrata',
      key = [host,id].join('::'); 

    koop.Cache.get( type, key, options, function(err, entry ){
      if ( err ){
        var url = host + self.socrata_path + id + '.json';
        var meta_url = host + self.socrata_view_path + id + '.json';
        //dmf: have to make a request to the views endpoint in order to get metadata
        var name;
        request.get(meta_url, function(err, data, response){
          if (err){
            callback(err, null)
          } else {
            name = JSON.parse( data.body ).name;
          }
        });
        request.get(url, function(err, data, response ){
          if (err) {
            callback(err, null);
          } else {
            var types = JSON.parse( data.headers['x-soda2-types'] );
              fields = JSON.parse( data.headers['x-soda2-fields'] );
            var locationField;
            types.forEach(function(t,i){
              if (t == 'location'){
                locationField = fields[i];
              } 
            });
            self.toGeojson( JSON.parse( data.body ), locationField, function(err, geojson){
              geojson.updated_at = new Date(data.headers['last-modified']).getTime();
              geojson.name = name;
              koop.Cache.insert( type, key, geojson, 0, function( err, success){
                if ( success ) callback( null, [geojson] );
              });
            });
          }
        });
      } else {
        callback( null, entry );
      }
    });

  };

  socrata.toGeojson = function(json, locationField, callback){
    if (!json || !json.length){
      callback('Error converting data to geojson', null);
    } else {
      var geojson = {type: 'FeatureCollection', features: []};
      var geojsonFeature;
      json.forEach(function(feature, i){
        geojsonFeature = {type: 'Feature', geometry: {}, id: i+1};
        if (feature && locationField){
          if (feature[locationField] && feature[locationField].latitude && feature[locationField].longitude){
            geojsonFeature.geometry.coordinates = [parseFloat(feature[locationField].longitude), parseFloat(feature[locationField].latitude)];
            geojsonFeature.geometry.type = 'Point';
            delete feature.location;
            geojsonFeature.properties = feature;
            geojson.features.push( geojsonFeature );
          } 
        } else if ( feature && feature.latitude && feature.longitude ){
           geojsonFeature.geometry.coordinates = [parseFloat(feature.longitude), parseFloat(feature.latitude)];
           geojsonFeature.geometry.type = 'Point';
           geojsonFeature.properties = feature;
           geojson.features.push( geojsonFeature );
        } else {
          geojsonFeature.geometry = null;
          geojsonFeature.properties = feature;
          geojson.features.push( geojsonFeature );
        }
      });
      callback(null, geojson);
    }
  };

  // compares the sha on the cached data and the hosted data
  // this method name is special reserved name that will get called by the cache model
  socrata.checkCache = function(key, data, options, callback){
    var self = this;
    var parts = key.split('::');
    url = parts[0] + this.socrata_path + parts[1] + '.json';

    var lapsed = (new Date().getTime() - data.updated_at);
    if (typeof(data.updated_at) == "undefined" || (lapsed > (1000*60*60))){
      callback(null, false);
    } else { 
      request.get(url, function( err, data, response ){
        if (err) {
          callback( err, null );
        } else {
          var types = JSON.parse( data.headers['x-soda2-types'] );
          var fields = JSON.parse( data.headers['x-soda2-fields'] );
          var locationField;
          types.forEach(function(t,i){
            if (t == 'location'){
              locationField = fields[i];
            }
          });
          self.toGeojson( JSON.parse( data.body ), locationField, function( error, geojson ){
            geojson.updated_at = new Date(data.headers['last-modified']).getTime();
            geojson.name = parts[1];
            callback( error, [geojson] );
          });
        }
      });
    }

  };

   // drops the item from the cache
  socrata.dropItem = function( host, itemId, options, callback ){
    var dir = [ 'socrata', host, itemId].join(':');
    koop.Cache.remove('socrata:'+host+':', itemId, options, function(err, res){
      koop.files.removeDir( 'files/' + dir, function(err, res){
        koop.files.removeDir( 'tiles/'+ dir, function(err, res){
          koop.files.removeDir( 'thumbs/'+ dir, function(err, res){
            callback(err, true);
          });
        });
      });
    });
  };

  return socrata;

};
  

module.exports = Socrata;
  
