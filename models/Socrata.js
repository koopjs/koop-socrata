var request = require('request'),
  async = require('async')

var Socrata = function (koop) {
  var socrata = koop.BaseModel(koop)

  // wrap request into a central place
  socrata.request = function (url, callback) {
    var options = {url: url}
    if (koop.config.socrata && koop.config.socrata.token) {
      options['X-App-Token'] = koop.config.socrata.token
    }
    request(options, callback)
  }

  // adds a service to the koop.Cache.db
  // needs a host, generates an id
  socrata.register = function (id, host, callback) {
    var type = 'socrata:services'
    koop.Cache.db.serviceCount(type, function (error, count) {
      if (error) {
        return callback(error, null)
      }
      id = id || count++
      koop.Cache.db.serviceRegister(type, {'id': id, 'host': host}, function (err, success) {
        callback(err, id)
      })
    })
  }

  socrata.remove = function (id, callback) {
    koop.Cache.db.serviceRemove('socrata:services', parseInt(id, 0) || id, callback)
  }

  // get service by id, no id == return all
  socrata.find = function (id, callback) {
    koop.Cache.db.serviceGet('socrata:services', parseInt(id, 0) || id, function (err, res) {
      if (err) {
        callback('No datastores have been registered with this provider yet. Try POSTing {"host":"url", "id":"yourId"} to /socrata', null)
      } else {
        callback(null, res)
      }
    })
  }

  socrata.socrata_path = '/resource/'
  // got the service and get the item
  socrata.getResource = function (host, hostId, id, options, callback) {
    var type = 'Socrata',
      key = id,
      table = [type, id, (options.layer || 0)].join(':'),
      locFieldName,
      urlid,
      paging = false,
      limit = 1000,
      fields,
      retGeoJSON,
      locationField

    // test id for '!' character indicating presence of a column name and handle
    if (id.indexOf('!') !== -1) {
      locFieldName = id.substring(id.indexOf('!') + 1, id.length)
      urlid = id.substring(0, id.indexOf('!'))
    } else {
      urlid = id
    }

    // request queue for paging data in a controlled way
    var pageQueue = async.queue(function (url, cb) {
      socrata.request(url, function (err, data, response) {
        if (err) {
          return callback(err)
        }
        // parse pages to GeoJSON and insert partial
        socrata.toGeojson(JSON.parse(data.body), locationField, fields, function (err, geojson) {
          if (err) {
            return callback(err)
          }
          koop.Cache.insertPartial(type, key, geojson, 0, function (err, success) {
            if (err) {
              return callback(err)
            }
            if (success) {
              // append geojson to return object
              retGeoJSON.features = retGeoJSON.features.concat(geojson.features)
              cb()
            }
          })
        })
      })
    }, 4)

    pageQueue.drain = function () {
      koop.Cache.getInfo(table, function (err, info) {
        if (err) {
          console.log('Could not get info', err)
          info = {}
        }
        delete info.status
        koop.Cache.updateInfo(table, info, function () {
          koop.log.debug('Finished paging ' + table)
        })
      })
    }

    // attempt to load from cache, if error perform new request and get first page
    koop.Cache.get(type, key, options, function (err, entry) {
      if (err) {
        var url = host + socrata.socrata_path + urlid + '.json?$order=:id&$limit=' + limit
        socrata.request(url, function (err, data, response) {
          if (err) {
            callback(err, null)
          } else {
            // test to see if paging will be needed later
            if (Object.keys(JSON.parse(data.body)).length === limit) {
              paging = true
            }
            // get name of location field
            try {
              if (locFieldName) {
                locationField = locFieldName
              } else {
                var types = JSON.parse(data.headers['x-soda2-types'])
                fields = JSON.parse(data.headers['x-soda2-fields'])
                types.forEach(function (t, i) {
                  if (t === 'location') {
                    locationField = fields[i]
                  }
                })
              }

              // parse first page to geoJSON and insert
              socrata.toGeojson(JSON.parse(data.body), locationField, fields, function (err, geojson) {
                if (err) {
                  return callback(err)
                }
                geojson.updated_at = new Date(data.headers['last-modified']).getTime()
                geojson.name = id
                geojson.host = {
                  id: hostId,
                  url: host
                }
                koop.Cache.insert(type, key, geojson, 0, function (err, success) {
                  if (err) {
                    return callback(err)
                  }
                  if (success) {
                    // check to see if paging is needed
                    if (paging === false) {
                      callback(null, [geojson])
                    } else {
                      // return as processing while we page
                      callback(null, [{status: 'processing'}])

                      // create GeoJSON return object
                      retGeoJSON = geojson
                      // detrmine count of table and needed pages
                      var count, pages, pageUrls = [],
                        countUrl = host + socrata.socrata_path + urlid + '.json?$select=count(*)'

                      socrata.request(countUrl, function (err, data, response) {
                        if (err) {
                          return callback(err)
                        }
                        count = parseInt(JSON.parse(data.body)[0].count, 10)
                        if ((count / limit) % 1 === 0) {
                          pages = (count / limit - 1)
                        } else {
                          pages = Math.floor(count / limit)
                        }
                        // page through data
                        for (var p = 1; p <= pages; p++) {
                          var page = [
                            host,
                            socrata.socrata_path,
                            urlid,
                            '.json?$order=:id&$limit=',
                            limit,
                            '&$offset=',
                            (p * limit)
                          ].join('')
                          pageUrls.push(page)
                        }

                        koop.Cache.getInfo(table, function (err, info) {
                          if (err) {
                            console.log('Could not get info', err)
                            info = {}
                          }
                          info.status = 'processing'
                          koop.Cache.updateInfo(table, info, function () {
                            pageQueue.push(pageUrls, function () {})
                          })
                        })
                      })
                    }
                  }
                })
              })
            } catch (e) {
              koop.log.error('Unable to parse response %s', url)
              callback(e, null)
            }
          }
        })
      } else {
        callback(null, entry)
      }
    })
  }

  socrata.toGeojson = function (json, locationField, fields, callback) {
    if (!json || !json.length) {
      callback('Error converting data to GeoJSON: JSON not returned from Socrata or blank JSON returned', null)
    } else {
      var geojson = { type: 'FeatureCollection', features: [] }
      var geojsonFeature,
        newFields = []
      json.forEach(function (feature, i) {
        var lat, lon
        geojsonFeature = { type: 'Feature', geometry: {}, id: i + 1 }

        if (feature && locationField && feature[locationField]) {
          lon = parseFloat(feature[locationField].longitude)
          lat = parseFloat(feature[locationField].latitude)
          if ((lon < -180 || lon > 180) || (lat < -90 || lat > 90)) {
            geojsonFeature.geometry = null
            geojsonFeature.properties = feature
            geojson.features.push(geojsonFeature)
          } else {
            geojsonFeature.geometry.coordinates = [lon, lat]
            geojsonFeature.geometry.type = 'Point'
            delete feature.location
            geojsonFeature.properties = feature
            geojson.features.push(geojsonFeature)
          }
        } else if (feature && feature.latitude && feature.longitude) {
          lon = parseFloat(feature.longitude)
          lat = parseFloat(feature.latitude)
          if ((lon < -180 || lon > 180) || (lat < -90 || lat > 90)) {
            geojsonFeature.geometry = null
            geojsonFeature.properties = feature
            geojson.features.push(geojsonFeature)
          } else {
            geojsonFeature.geometry.coordinates = [lon, lat]
            geojsonFeature.geometry.type = 'Point'
            geojsonFeature.properties = feature
            geojson.features.push(geojsonFeature)
          }
        } else {
          geojsonFeature.geometry = null
          geojsonFeature.properties = feature
          geojson.features.push(geojsonFeature)
        }

        // make sure each feature has flattened object props
        fields.forEach(function (f) {
          if (f.substring(0, 1) !== ':') {
            if (typeof geojson.features[i].properties[f] === 'object') {
              for (var v in geojson.features[i].properties[f]) {
                var newAttr = f + '_' + v
                geojson.features[i].properties[newAttr] = geojson.features[i].properties[f][v]
                newFields.push(newAttr)
              }
              delete geojson.features[i].properties[f]
            }
          }
        })
      })
      // 2nd loop over the data to ensure all new fields are present in each feature
      if (newFields && newFields.length) {
        geojson.features.forEach(function (feature) {
          newFields.forEach(function (field) {
            if (!feature.properties[field]) {
              feature.properties[field] = null
            }
          })
        })
      }
      callback(null, geojson)
    }
  }

  // compares the sha on the cached data and the hosted data
  // this method name is special reserved name that will get called by the cache model
  socrata.checkCache = function (key, data, options, callback) {
    var url = data.host + this.socrata_path + key + '.json'
    var lapsed = (new Date().getTime() - data.updated_at)
    if (typeof (data.updated_at) === 'undefined' || (lapsed > (1000 * 60 * 60))) {
      callback(null, false)
    } else {
      socrata.request(url, function (err, data, response) {
        if (err) {
          callback(err, null)
        } else {
          var types = JSON.parse(data.headers['x-soda2-types'])
          var fields = JSON.parse(data.headers['x-soda2-fields'])
          var locationField
          types.forEach(function (t, i) {
            if (t === 'location') {
              locationField = fields[i]
            }
          })
          socrata.toGeojson(JSON.parse(data.body), locationField, fields, function (error, geojson) {
            geojson.updated_at = new Date(data.headers['last-modified']).getTime()
            geojson.name = data.name || key
            geojson.host = data.host
            callback(error, [geojson])
          })
        }
      })
    }
  }

  // drops the item from the cache
  socrata.dropItem = function (host, itemId, options, callback) {
    var dir = [ 'socrata', host, itemId].join(':'),
    errors = []

    koop.Cache.remove('Socrata', itemId, options, function (err, res) {
      if (err) errors.push(err)
      koop.files.removeDir('files/' + dir, function (err, res) {
        if (err) errors.push(err)
        koop.files.removeDir('tiles/' + dir, function (err, res) {
          if (err) errors.push(err)
          koop.files.removeDir('thumbs/' + dir, function (err, res) {
            if (err) errors.push(err)
            callback(errors.join(', '), true)
          })
        })
      })
    })
  }

  socrata.getCount = function (key, options, callback) {
    koop.Cache.getCount(key, options, callback)
  }

  return socrata

}

module.exports = Socrata
