var request = require('request');

var Socrata = function( koop ){

  var socrata = {};
  socrata.__proto__ = koop.BaseModel( koop );

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
    koop.Cache.db.serviceGet( 'socrata:services', parseInt(id) || id, function(err, res){
      if (err){
        callback('No datastores have been registered with this provider yet. Try POSTing {"host":"url", "id":"yourId"} to /socrata', null);
      }
      else {
        callback(null, res);
      }
    });
  };

  socrata.socrata_path = '/resource/';

  // got the service and get the item
  socrata.getResource = function( host, hostId, id, options, callback ){
    var self = this,
      type = 'Socrata',
      key = id,
      locFieldName,
      urlid,
      paging = false,
      limit = 1000;

    // test id for '!' character indicating presence of a column name and handle
    if (id.indexOf("!") != -1){
      locFieldName = id.substring(id.indexOf("!") + 1,id.length);
      urlid = id.substring(0, id.indexOf("!"));
    }
    else {
      urlid = id;
    }

    // attempt to load from cache, if error perform new request and get first page
    koop.Cache.get( type, key, options, function(err, entry ){
      if ( err ){
        var url = host + self.socrata_path + urlid + '.json?$order=:id&$limit=' + limit;
        request.get(url, function(err, data, response ){
          if (err) {
            callback(err, null);
          } else {      

            // test to see if paging will be needed later
            if (Object.keys(JSON.parse(data.body)).length == limit){
              paging = true;
            }

            // get name of location field
            try {
              var locationField;
              if (locFieldName){
                locationField = locFieldName;
              }
              else {
                var types = JSON.parse( data.headers['x-soda2-types'] );
                var fields = JSON.parse( data.headers['x-soda2-fields'] );
                types.forEach(function(t,i){
                  if (t == 'location'){
                    locationField = fields[i];
                  }
                });
              }

              // parse first page to geoJSON and insert
              self.toGeojson( JSON.parse( data.body ), locationField, function(err, geojson){
                geojson.updated_at = new Date(data.headers['last-modified']).getTime();
                geojson.name = id;
                geojson.host = {
                  id: hostId,
                  url: host
                };
                koop.Cache.insert( type, key, geojson, 0, function( err, success){
                  if ( success ) {
                    // check to see if paging is needed
                    if (paging === false){
                      callback( null, [geojson] );  
                    }
                    else {
                      // create GeoJSON return object
                      retGeoJSON = geojson;
                      // detrmine count of table and needed pages
                      var count, pages;
                      var pagesComplete = 0;
                      var countUrl = host + self.socrata_path + urlid + '.json?$select=count(*)';
                      request.get(countUrl, function(err, data, response){
                        count = parseInt(JSON.parse(data.body)[0].count,10);
                        if ((count/limit) % 1 === 0){
                          pages = (count/limit - 1);
                        }
                        else {
                          pages = Math.floor(count/limit);
                        }
                        // page through data
                        for (var p = 1; p <= pages; p++){
                          var pUrl = host + self.socrata_path + urlid + '.json?$order=:id&$limit=' + limit + '&$offset=' + (p*limit);
                          request.get(pUrl,function(err, data, response){
                            // parse pages to GeoJSON and insert partial
                            self.toGeojson( JSON.parse( data.body ), locationField, function(err, geojson){
                              geojson.updated_at = new Date(data.headers['last-modified']).getTime();
                              geojson.name = id;
                              geojson.host = {
                                id: hostId,
                                url: host
                              };
                              koop.Cache.insertPartial( type, key, geojson, 0, function( err, success){
                                if ( success ) {
                                  // append geojson to return object
                                  for (f = 0; f < geojson.features.length; f++){
                                    retGeoJSON.features.push(geojson.features[f]);
                                  }
                                  // update pages completed and check for completion of pages
                                  pagesComplete++;
                                  checkDone();
                                }
                              });
                            });
                          });
                        }

                        // function to check completion of pages
                        var checkDone = function(){
                          if (pagesComplete == pages){
                            callback( null, [retGeoJSON])
                          }
                          else {
                          }
                        };
                      });
                    }
                  }
                });
              });
            } catch (e){
              console.log('Error?', e);
              koop.log.error('Unable to parse response %s', url);
              callback(e, null); 
            }
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
    var url = data.host + this.socrata_path + key + '.json';

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
            geojson.name = data.name || key;
            geojson.host = data.host;
            callback( error, [geojson] );
          });
        }
      });
    }

  };

   // drops the item from the cache
  socrata.dropItem = function( host, itemId, options, callback ){
    var dir = [ 'socrata', host, itemId].join(':');
    koop.Cache.remove('Socrata', itemId, options, function(err, res){
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

