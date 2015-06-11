var request = require('request'),
  async = require('async'),
  JSONStream = require('JSONStream'),
  es = require('event-stream'),
  ogr2ogr = require('ogr2ogr')

var Socrata = function (koop) {
  var socrata = koop.BaseModel(koop)
  // set up this object to hold info on what is currently processing
  var processing = {}
  socrata.pageLimit = 10000
  socrata.resourcePath = '/resource/'
  socrata.viewPath = '/views/'
  socrata.expirationPeriod = (1000 * 60 * 60 * 24)
  if (koop.config && koop.config.socrata) {
    if (koop.config.socrata.pageLimit) {
      socrata.pageLimit = koop.config.socrata.pageLimit
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
      firstRow = [],
      errors = []

    // test id for '!' character indicating presence of a column name and handle
    if (id.indexOf('!') !== -1) {
      urlId = id.substring(0, id.indexOf('!'))
    } else {
      urlId = id
    }

    // request queue for paging data in a controlled way
    var pageQueue = async.queue(function (url, callback) {
      koop.log.info('Processing: ' + url)
      socrata.processStream(socrata.getPage(url), meta, function (err, geojson) {
        if (err) {
          callback('Failed while processing a page of ' + table + '. ' + err)
        } else {
          koop.Cache.insertPartial(type, key, geojson, 0, function (err, success) {
            if (err) {
              callback('Failed while inserting a page of ' + table + '. ' + err)
            } else if (success) {
              callback(null)
            }
          })
        }
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
          // remove this dataset from the global processing object
          delete processing[host + id]
        })
      })
    }

    // request queue for gathering info on a dataset
    var infoQueue = async.queue(function (job, cb) {
      job.task(job.host, job.urlId, cb)
    }, 3)

    // when the infoQueue drains we will use the feedback from the first three socrata API calls to decide how to proceed
    infoQueue.drain = function () {
      // if the dataset is only one page long and getting the first row fails
      // we can still grab it by requesting the whole dataset in one go
      if (!errors.length) {
        // proceed along the happy path
        socrata.ingestResource(host, urlId, meta, firstRow, pageQueue, function (err, info) {
          if (err) {
            koop.log.error(err)
          } else {
            koop.log.info(info)
          }
        })
      } else if (errors.length === 1 && errors[0].split('::')[0] === 'first') {
        socrata.ingestResourceFallback(host, urlId, meta, function (err, info) {
          if (err) {
            koop.log.error(err)
          } else {
            koop.log.info(info)
          }
        })
        // maybe we have errors because this is a blob file, we can just request the whole thing if it's a zip
      } else if (errors.length && meta.blobFilename && meta.blobFilename.split('.')[1] === 'zip') {
        socrata.processZip(host, urlId, function (err, geojson) {
          if (err) {
            koop.log.error(err)
          } else {
            socrata.insert(key, meta, geojson, function (err, success) {
              if (err) {
                koop.log.error('Processing failed for zip resource: ' + 'Socrata:' + key + ':0', err)
                // handle err
              } else {
                koop.log.info('Processing completed for zip resource: ' + 'Socrata:' + key + ':0')
              }
            })
          }
        })
      // if count is the only thing that failed we can still try to grab the dataset in one go
      } else if (errors.length === 1 && errors[0].split('::')[0] === 'count') {
        socrata.ingestResourceFallback(host, urlId, meta, function (err, info) {
          if (err) {
            koop.log.error(err)
          } else {
            koop.log.info(info)
          }
        })
      } else {
        socrata.setFail(table, errors, function () {
          koop.log.info('Processing failed on ' + 'Socrata:' + key + ':0', errors.join(', '))
          delete processing[host + id]
        })
      }
    }

    koop.Cache.get(type, key, options, function (err, entry) {
      if (err || (entry.length && entry[0].status === 'processing')) {
        koop.Cache.getInfo(table, function (error, info) {
          if (error && !processing[host + key]) {
            // we don't have the data and it's not processing return processing and make a new request
            processing[host + id] = true
            callback(null, [{status: 'processing'}])
            infoQueue.push({
                task: socrata.getRowCount,
                host: host,
                urlId: urlId
            }, function (err, rowCount) {
                  if (err) {
                    koop.log.error('Could not get rowCount. ' + err)
                    errors.push(err)
                  } else {
                    meta.rowCount = rowCount
                  }
                }
            )
            infoQueue.push({
                task: socrata.getMeta,
                host: host,
                urlId: urlId
            }, function (err, info) {
                  if (err) {
                    koop.log.error('Could not get metadata. ' + err)
                    errors.push(err)
                  } else {
                    meta.name = info.name
                    meta.location_field = info.location_field
                    meta.updated_at = info.updated_at
                    meta.fields = info.fields
                    meta.blobFilename = info.blobFilename
                  }
                }
            )
            infoQueue.push({
                task: socrata.getFirst,
                host: host,
                urlId: urlId
            }, function (err, data) {
                  if (err) {
                    koop.log.error('Could not get first row. ' + err)
                    errors.push(err)
                  } else {
                    firstRow = data
                  }
                }
            )
          } else if (error & processing[host + key]) {
            // we don't have any info yet, but it is processing just callback
            callback(null, [{status: 'processing'}])
          } else {
            // we have info either an error or some progress
            callback(null, [info])
          }
        })
      } else {
        // we have the data or info so send it back
        callback(null, entry)
        // after data has been sent, check for expiration
        socrata.checkExpiration(host, urlId, function (err, expired) {
          if (err) {
            koop.log.error('Unable to check updated date for: ' + host + urlId)
          } else if (expired) {
            koop.log.info('Cache expired, regenerating. ' + host + urlId)
          }
        })
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
      if (err) {
        callback(err)
      } else if (res.statusCode !== 200) {
        callback('count::' + options.url + '::' + res.statusCode, null)
      } else {
        var rowCount
        try {
          rowCount = JSON.parse(body)[0].count
        } catch (e) {
          err = 'Could not parse count JSON'
        }
        callback(err, rowCount)
      }
    })
  }

  socrata.getMeta = function (host, id, callback) {
    var meta = {}
    var metaUrl = host + socrata.viewPath + id + '.json'
    var options = {url: metaUrl, gzip: true}
    request(options, function (err, res, body) {
      if (err) {
        callback(err)
      } else if (res.statusCode !== 200) {
        callback('meta::' + options.url + '::' + res.statusCode)
      } else {
        var response = JSON.parse(body)
        meta.blobFilename = response.blobFilename
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
      }
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
        callback(err)
      } else if (res.statusCode !== 200) {
        callback('first::' + options.url + '::' + res.statusCode)
      } else {
        callback(err, JSON.parse(body))
      }
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

  socrata.processZip = function (host, id, callback) {
    var zipUrl = host + '/api/geospatial/' + id + '?method=export&format=Original'
    var options = {url: zipUrl, gzip: true}
    if (socrata.token) {
      options.headers = {'X-App-Token': socrata.token}
    }
    socrata.ogrZip(request(options), callback)
    // to do error handling
  }

  socrata.ogrZip = function (stream, callback) {
    ogr2ogr(stream, 'ESRI Shapefile').exec(function (err, data) {
      callback(err, data)
    })
  }

  socrata.processStream = function (dataStream, meta, callback) {
    var geojson = { type: 'FeatureCollection', features: [] }
    dataStream
    .pipe(JSONStream.parse('*'))
    .pipe(es.map(function (data, cb) {
      socrata.toGeojson([data], meta.location_field, meta.fields, function (err, converted) {
        if (err) {
          cb(err)
        }
        geojson.features.push(converted.features[0])
        cb()
      })
    }))
    .on('error', function (err) {
      callback(err)
    })
    .on('end', function () {
      callback(null, geojson)
    })
  }

  socrata.ingestResource = function (host, id, meta, firstRow, pageQueue, callback) {
    socrata.toGeojson(firstRow, meta.location_field, meta.fields, function (err, geojson) {
      if (err) {
        callback('Failed to parse the first row at: ' + host + id + '. ' + err)
        // update status as failed
      } else {
        meta.status = 'processing'
        socrata.insert(id, meta, geojson, function (err, success) {
          if (err) {
            callback('First row insert failed for: ' + host + id + '. ' + err)
          } else {
            var pages = socrata.buildPages(host, id, meta.rowCount)
            pageQueue.push(pages, function (err) {
              if (err) {
                callback(err)
              } else {
                callback(null, 'Beginning to page through ' + host + '/resource' + id + ' ' + pages.length + ' Pages.')
              }
            })
          }
        })
      }
    })
  }

  socrata.ingestResourceFallback = function (host, id, meta, callback) {
    var url = host + socrata.resourcePath + id + '.json'
    socrata.processStream(socrata.getPage(url), meta, function (err, geojson) {
      if (err) {
        callback(err)
      } else {
        meta.status = 'complete'
        socrata.insert(id, meta, geojson, function (err, success) {
          if (err) {
            callback('Fallback method failed for: ' + url + '. ' + err)
          } else {
            callback(null, 'Fallback method succeeded for: ' + url)
            // remove this dataset from the processing object
            delete processing[host + id]
          }
        })
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
  socrata.checkExpiration = function (host, id, callback) {
    var table = ['Socrata', id, 0].join(':')
    koop.Cache.getInfo(table, function (err, info) {
      if (err) {
        callback(err)
      }
      // no info means it's the first request for this resource, no need to check cache
      if (info) {
        // check if enough time has passed to call to Socrata
        if ((new Date() - info.checked_at) > socrata.expirationPeriod) {
          socrata.getMeta(host, id, function (err, meta) {
            if (err) {
              callback(err)
            }
            // now check if the resource expired
            if (meta.updated_at > info.updated_at) {
              // it's expired so remove the item and go fetch it again
              socrata.dropItem(host, id, 0, function (err, success) {
                if (err) {
                  callback(err)
                }
                socrata.getResource(host, id, 0, {}, function (err, res) {
                  if (err) {
                    callback(err)
                  }
                  callback(null, true)
                })
              })
            } else {
              callback(null, false)
            }
          })
        } else {
          callback(null, false)
        }
      }
    })
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
        info = {}
      }
      info.checked_at = new Date()
      // we don't want to save any status on completed datasets
      if (newInfo.status !== 'complete') {
        info.status = newInfo.status
      }
      if (newInfo.errors) {
        info.errors = newInfo.errors
      }
      koop.Cache.updateInfo(table, info, function (err, success) {
        if (err) {
          callback(err)
        }
        callback(null, true)
      })
    })
  }

  socrata.insert = function (key, meta, geojson, callback) {
    var table = ['Socrata', key, 0].join(':')
    geojson.name = meta.name
    geojson.location_field = meta.location_field
    geojson.updated_at = meta.updated_at
    koop.Cache.insert('Socrata', key, geojson, 0, function (err, success) {
      if (err) {
        callback(err, false)
      } else {
        meta.checked_at = new Date()
        socrata.updateInfo(table, meta, function (err, success) {
          if (err) {
            callback(err, false)
          } else {
            callback(null, true)
          }
        })
      }
    })
  }

  socrata.setFail = function (table, errors, callback) {
    var feature = { type: 'Feature', geometry: null, id: 0 }
    var geojson = { type: 'FeatureCollection', features: [feature] }
    var key = table.split(':')[1]
    // create a table with blank features so we can attach a failed status
    koop.Cache.insert('Socrata', key, geojson, 0, function (err, success) {
      if (err) {
        callback(err)
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
