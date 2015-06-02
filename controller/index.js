var SphericalMerc = require('sphericalmercator'),
  merc = new SphericalMerc({ size: 256 }),
  fs = require('fs'),
  crypto = require('crypto')

// a function that is given an instance of Koop at init
var Controller = function (Socrata, BaseController) {
  var controller = BaseController()

  // register a socrata instance
  controller.register = function (req, res) {
    if (!req.body.host) {
      res.send('Must provide a host to register:', 500)
    } else {
      Socrata.register(req.body.id, req.body.host, function (err, id) {
        if (err) {
          res.send(err, 500)
        } else {
          res.json({ 'serviceId': id })
        }
      })
    }
  }

  controller.list = function (req, res) {
    Socrata.find(null, function (err, data) {
      if (err) {
        res.send(err, 500)
      } else {
        res.json(data)
      }
    })
  }

  controller.find = function (req, res) {
    Socrata.find(req.params.id, function (err, data) {
      if (err) {
        res.send(err, 404)
      } else {
        res.json(data)
      }
    })
  }

  // drops the cache for an item
  controller.drop = function (req, res) {
    Socrata.find(req.params.id, function (err, data) {
      if (err) {
        res.send(err, 500)
      } else {
        // Get the item
        Socrata.dropItem(data.host, req.params.item, req.query, function (error, itemJson) {
          if (error) {
            res.send(error, 500)
          } else {
            res.json(itemJson)
          }
        })
      }
    })
  }

  controller.findResource = function (req, res) {
    Socrata.find(req.params.id, function (err, data) {
      if (err) {
        res.send(err, 500)
      } else {
        // Get the item
        Socrata.getResource(data.host, req.params.id, req.params.item, req.query, function (error, itemJson) {
          // return 202 when processing
          if (itemJson && itemJson.length && itemJson[0].status === 'processing') {
            Socrata.getCount(['Socrata', req.params.item, (req.query.layer || 0)].join(':'), req.query, function (err, count) {
              if (err) {
                console.log('Could not socrata feature count', req.params.item)
              }
              return res.status(202).json({status: 'processing', count: count})
            })
          } else if (error) {
            res.status(500).send(error)
          } else if (req.params.format) {
            // change geojson to json
            req.params.format = req.params.format.replace('geojson', 'json')

            var dir = ['socrata', req.params.id, req.params.item ].join(':')
            // build the file key as an MD5 hash that's a join on the paams and look for the file
            var toHash = JSON.stringify(req.params) + JSON.stringify(req.query)
            var key = crypto.createHash('md5').update(toHash).digest('hex')

            var path = ['files', dir].join('/')
            var fileName = key + '.' + req.params.format
            Socrata.files.exists(path, fileName, function (exists, path) {
              if (exists) {
                if (path.substr(0, 4) === 'http') {
                  res.redirect(path)
                } else {
                  res.sendfile(path)
                }
              } else {
                Socrata.exportToFormat(req.params.format, dir, key, itemJson[0], {}, function (err, file) {
                  if (err) {
                    res.send(err, 500)
                  } else {
                    if (file.substr(0, 4) === 'http') {
                      res.redirect(file)
                    } else {
                      res.sendfile(file)
                    }
                  }
                })
              }
            })
          } else {
            var geojson = itemJson[0]
            if (geojson && geojson.features && geojson.features.length) {
              geojson.features = geojson.features.slice(0, req.query.limit || 100)
            }
            res.json(geojson)
          }
        })
      }
    })
  }

  controller.del = function (req, res) {
    if (!req.params.id) {
      res.send('Must specify a service id', 500)
    } else {
      Socrata.remove(req.params.id, function (err, data) {
        if (err) {
          res.send(err, 500)
        } else {
          res.json(data)
        }
      })
    }
  }

  controller.featureserver = function (req, res) {
    var callback = req.query.callback
    delete req.query.callback

    for (var k in req.body) {
      req.query[k] = req.body[k]
    }

    Socrata.find(req.params.id, function (err, data) {
      if (err) {
        res.send(err, 500)
      } else {
        // Get the item
        req.query.limit = req.query.limit || req.query.resultRecordCount || 1000000000
        req.query.offset = req.query.resultOffset || null
        Socrata.getResource(data.host, req.params.id, req.params.item, req.query, function (error, geojson) {
          if (error) {
            res.send(error, 500)
          } else if (geojson[0] && geojson[0].status === 'processing') {
            res.send(geojson, 202)
          } else {
            // pass to the shared logic for FeatureService routing
            delete req.query.geometry
            delete req.query.where
            controller.processFeatureServer(req, res, err, geojson, callback)
          }
        })
      }
    })
  }

  controller.tiles = function (req, res) {
    var callback = req.query.callback
    delete req.query.callback

    var key,
      layer = req.params.layer || 0

    var _send = function (err, data) {
      if (err) {
        res.status(404).send(err)
        return
      }
      req.params.key = key + ':' + layer
      if (req.query.style) {
        req.params.style = req.query.style
      }
      Socrata.tileGet(req.params, data[ layer ], function (err, tile) {
        if (err) {
          res.status(404).send(err)
          return
        }
        if (req.params.format === 'png' || req.params.format === 'pbf') {
          res.sendfile(tile)
        } else {
          if (callback) {
            res.send(callback + '(' + JSON.stringify(tile) + ')')
          } else {
            if (typeof tile === 'string') {
              res.sendfile(tile)
            } else {
              res.json(tile)
            }
          }
        }
      })
    }
    // build the geometry from z,x,y
    var bounds = merc.bbox(req.params.x, req.params.y, req.params.z)
    req.query.geometry = {
      xmin: bounds[0],
      ymin: bounds[1],
      xmax: bounds[2],
      ymax: bounds[3],
      spatialReference: { wkid: 4326 }
    }

    var _sendImmediate = function (file) {
      if (req.params.format === 'png' || req.params.format === 'pbf') {
        res.sendfile(file)
      } else {
        fs.readFile(file, function (err, data) {
          if (err) {
            res.status(404).send(err)
            return
          }
          if (callback) {
            res.send(callback + '(' + JSON.parse(data) + ')')
          } else {
            res.json(JSON.parse(data))
          }
        })
      }
    }

    key = ['socrata', req.params.id, req.params.item].join(':')
    var file = Socrata.files.localDir + '/tiles/'
    file += key + '/' + req.params.format
    file += '/' + req.params.z + '/' + req.params.x + '/' + req.params.y + '.' + req.params.format
    var jsonFile = file.replace(/png|pbf|utf/g, 'json')
    // if the json file alreadty exists, dont hit the db, just send the data
    if (fs.existsSync(jsonFile) && !fs.existsSync(file)) {
      _send(null, fs.readFileSync(jsonFile))
    } else if (!fs.existsSync(file)) {
      Socrata.find(req.params.id, function (err, data) {
        if (err) {
          res.send(err, 500)
        } else {
          // Get the item
          Socrata.getResource(data.host, req, req.params.id, req.params.item, req.query, _send)
        }
      })
    } else {
      _sendImmediate(file)
    }
  }

  // need a thumbnail? get it here...
  controller.thumbnail = function (req, res) {
    var key = ['socrata', req.params.id, req.params.item].join(':')
    var dir = Socrata.files.localDir + '/thumbs/'
    req.query.width = parseInt(req.query.width, 0) || 150
    req.query.height = parseInt(req.query.height, 0) || 150
    req.query.f_base = dir + key + '/' + req.query.width + '::' + req.query.height
    var fileName = Socrata.thumbnailExists(key, req.query)

    if (fileName) {
      res.sendfile(fileName)
    } else {
      Socrata.find(req.params.id, function (err, data) {
        if (err) {
          res.send(err, 500)
        } else {
          // Get the item
          Socrata.getResource(data.host, req.params.id, req.params.item, req.query, function (error, itemJson) {
            if (error) {
              res.send(error, 500)
            } else {
              var key = ['socrata', req.params.id, req.params.item].join(':')
              // generate a thumbnail
              Socrata.thumbnailExists(itemJson[0], key, req.query, function (err, file) {
                if (err) {
                  res.send(err, 500)
                } else {
                  // send back image
                  res.sendfile(file)
                }
              })
            }
          })
        }
      })
    }
  }

  controller.preview = function (req, res) {
    res.render(__dirname + '/../views/demo', {
      locals: {
        host: req.params.id,
        item: req.params.item
      }
    })
  }

  return controller
}

module.exports = Controller
