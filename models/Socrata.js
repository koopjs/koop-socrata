var request = require('request'),
  async = require('async'),
  JSONStream = require('JSONStream'),
  es = require('event-stream')

var Socrata = function (koop) {
  var socrata = koop.BaseModel(koop)
  socrata.pageLimit = 1000
  socrata.resourcePath = '/resource/'
  socrata.viewPath = '/views/'
  if (koop.config && koop.config.socrata) {
    if (koop.config.socrata.reqLimit) {
      socrata.pageLimit = koop.config.socrata.reqLimit
    }
    if (koop.config.socrata.token) {
      socrata.token = koop.config.socrata.token
    }
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

  // got the service and get the item
  socrata.getResource = function (host, hostId, id, options, callback) {
    var type = 'Socrata',
      key = id,
      table = [type, id, (options.layer || 0)].join(':'),
      urlId,
      meta = {},
      firstRow,
      errors = []

    // test id for '!' character indicating presence of a column name and handle
    if (id.indexOf('!') !== -1) {
      urlId = id.substring(0, id.indexOf('!'))
    } else {
      urlId = id
    }

    // request queue for paging data in a controlled way
    var pageQueue = async.queue(function (url, cb) {
      var geojson = { type: 'FeatureCollection', features: [] }
      koop.log.info('Processing: ' + url)
      socrata.getPage(url)
      .pipe(JSONStream.parse('*'))
      .pipe(es.map(function (data, callback) {
        socrata.toGeojson([data], meta.location_field, meta.fields, function (err, converted) {
          if (err) {
            callback(err)
          }
          geojson.features.push(converted.features[0])
          callback()
        })
      }))
      .on('error', function (err) {cb(err)})
      .on('end', function () {
        koop.Cache.insertPartial(type, key, geojson, 0, function (err, success) {
          if (err) {
            koop.log.error('Failed while inserting a page of Socrata:' + key + ':0')
            cb(err)
          } else if (success) {
            koop.log.info('Successfully inserted a page of Socrata:' + key + ':0')
            cb(null)
          }
        })
      })
    }, 4)

    pageQueue.drain = function () {
      koop.Cache.getInfo(table, function (err, info) {
        if (err) {
          koop.log.error('Could not get info of ' + table, err)
          info = {}
        }
        delete info.status
        koop.Cache.updateInfo(table, info, function () {
          koop.log.info('Finished paging ' + table)
        })
      })
    }

    // request queue for gathering info on a dataset
    var infoQueue = async.queue(function (job, cb) {
      job.task(job.host, job.urlId, cb)
    }, 3)

    infoQueue.drain = function () {
      if (errors.length) {
        socrata.setFail(table, errors, function () {
          koop.log.info('Processing failed on' + 'Socrata:' + key + ':0', errors)
          return
        })
      }
      // insert the first row to create the table and set things off
      socrata.toGeojson(firstRow, meta.location_field, meta.fields, function (err, geojson) {
        if (err) {
          // update status as failed
          return
        }
        geojson.name = meta.name
        geojson.location_field = meta.location_field
        geojson.updated_at = meta.updated_at
        koop.Cache.insert(type, key, geojson, 0, function (err, success) {
          if (err) {
            // todo update status as failed
            return
          }
          koop.Cache.getInfo(table, function (err, info) {
            if (err) {
              koop.log.error('Could not get info for ' + 'Socrata:' + key + ':0', err)
              info = {}
            }
            info.status = 'processing'
            koop.Cache.updateInfo(table, info, function () {
              // now that we have the table structure and the status is set as processing we can go get the rest of the data
              var pages = socrata.buildPages(host, id, meta.rowCount)
              koop.log.info('Beginning to page through ' + host + '/resource' + id + ' ' + pages.length + ' Pages.')
              pages.forEach(function (page) {
                pageQueue.push(page, function (err) {
                  if (err) {
                    koop.log.error(page, err)
                  }
                })
              })
            })
          })
        })
      })
    }

    // attempt to load from cache, if error perform new request
    koop.Cache.get(type, key, options, function (err, entry) {
      if (err || entry[0].status === 'processing') {
        // check for information about this dataset
        koop.Cache.getInfo(table, function (err, info) {
          if (err) {
            // Return processing immediately before doing other work
            callback(null, [{status: 'processing'}])
          } else if (!err && info.status) {
            callback(null, info)
            return
          }
        })
        // Simultaneously gather count and metadata by adding them to the queue with their variables
        infoQueue.push({
            task: socrata.getRowCount,
            host: host,
            urlId: urlId
        }, function (err, rowCount) {
              if (err) {
                // set status to failed
                errors.push(err)
                return
              }
              meta.rowCount = rowCount
            })
        infoQueue.push({
            task: socrata.getMeta,
            host: host,
            urlId: urlId
        }, function (err, info) {
              if (err) {
                // set status to failed
                errors.push(err)
                return
              }
              meta.name = info.name
              meta.location_field = info.location_field
              meta.updated_at = info.updated_at
              meta.fields = info.fields
            })
        infoQueue.push({
            task: socrata.getFirst,
            host: host,
            urlId: urlId
        }, function (err, data) {
              if (err) {
                // set status to failed
                errors.push(err)
                return
              }
              firstRow = data
            })
      } else {
        callback(null, entry)
      }
    })
  }

  socrata.getRowCount = function (host, id, callback) {
    var countUrl = host + socrata.resourcePath + id + '.json?$select=count(*)'
    var options = {url: countUrl, gzip: true}
    if (socrata.token) {
      options.headers = {'X-App-Token': socrata.token}
    }
    request(options, function (err, res, body) {
      koop.log.debug('status of count:' + res.statusCode)
      if (err) {
        koop.log.error('Could not get count at: ' + options.url, err)
      } else if (res.statusCode !== 200) {
        callback('GetRowCount::' + options.url + '::' + res.statusCode, null)
        return
      }
      var rowCount = JSON.parse(body)[0].count
      callback(err, rowCount)
    })
  }

  socrata.getMeta = function (host, id, callback) {
    var meta = {}
    var metaUrl = host + socrata.viewPath + id + '.json'
    var options = {url: metaUrl, gzip: true}
    request(options, function (err, res, body) {
      koop.log.debug('status of meta:' + res.statusCode)
      if (err) {
        koop.log.error('Could not get metadata at: ' + options.url, err)
      } else if (res.statusCode !== 200) {
        callback('GetMeta::' + options.url + '::' + res.statusCode)
        return
      }
      var response = JSON.parse(body)
      meta.updated_at = new Date(res.headers['last-modified']).getTime()
      meta.name = response.name
      meta.fields = []
      response.columns.forEach(function (col) {
        meta.fields.push(col.fieldName)
        if (col.dataTypeName === 'location') {
          meta.location_field = col.fieldName
        }
      })
      callback(err, meta)
    })
  }

  socrata.getFirst = function (host, id, callback) {
    var firstUrl = host + socrata.resourcePath + id + '.json?$order=:id&$limit=1'
    var options = {url: firstUrl, gzip: true}
    if (socrata.token) {
      options.headers = {'X-App-Token': socrata.token}
    }
    request(options, function (err, res, body) {
      if (err) {
        koop.log.error('failed to get first row at: ' + host + socrata.resourcePath + id + '.json?$order=:id&$limit=1')
      } else if (res.statusCode !== 200) {
        callback('GetFirst::' + options.url + '::' + res.statusCode)
        return
      }
      callback(err, JSON.parse(body))
    })
  }

  socrata.buildPages = function (host, id, rowCount) {
    var urls = []
    var pageCount = Math.ceil((rowCount - 1) / socrata.pageLimit)
    for (var p = 0; p < pageCount; p++) {
      var page = host + socrata.resourcePath + id + '.json?$order=:id&$limit=' + socrata.pageLimit + '&$offset=' + ((p * socrata.pageLimit) + 1)
      urls[p] = page
    }
    return urls
  }

  socrata.getPage = function (pageUrl) {
    var options = {url: pageUrl, gzip: true}
    if (socrata.token) {
      options.headers = {'X-App-Token': socrata.token}
    }
    // Return the stream so it can be piped to a parser
    return request(options)
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
    var url = data.host + socrata.resourcePath + key + '.json'
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

  socrata.updateInfo = function (table, newInfo, callback) {
    koop.Cache.getInfo(table, function (err, info) {
      if (err) {
        koop.log.debug('No info found for: ' + table)
        info = {}
      }
      info.status = newInfo.status
      if (newInfo.errors) {
        info.errors = newInfo.errors
      }
      koop.Cache.updateInfo(table, info, function (err, success) {
        if (err) {
          console.error('failed to update info')
        }
        koop.log.debug('Updated: ' + table + 'with: ' + info)
        callback()
      })
    })
  }

  socrata.setFail = function (table, errors, callback) {
    var feature = { type: 'Feature', geometry: null, id: 0 }
    var geojson = { type: 'FeatureCollection', features: [feature] }
    var key = table.split(':')[1]
    // create a table with blank features so we can attach a failed status
    koop.Cache.insert('Socrata', key, geojson, 0, function (err, success) {
      if (err) {
        console.error('insert empty table error', err)
      }
      var info = {}
      info.status = 'processing'
      info.errors = errors
      socrata.updateInfo(table, info, callback)
    })
  }

  socrata.getCount = function (key, options, callback) {
    koop.Cache.getCount(key, options, callback)
  }

  return socrata

}

module.exports = Socrata
