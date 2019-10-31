/**
 * Much of this code was written by noffle in the kappa-osm module.
 * see: https://github.com/digidem/kappa-osm
 **/
module.exports = EchoDB

var randomBytes = require('randombytes')
var sub = require('subleveldown')
var once = require('once')
var EventEmitter = require('events').EventEmitter
var through = require('through2')
var pumpify = require('pumpify')

var umkv = require('unordered-materialized-kv')
var createKvIndex = require('./lib/kv-index.js')

function EchoDB (opts) {
  if (!(this instanceof EchoDB)) return new EchoDB(opts)
  if (!opts.core) throw new Error('missing param "core"')
  if (!opts.index) throw new Error('missing param "index"')
  if (!opts.storage) throw new Error('missing param "storage"')

  var self = this

  this.core = opts.core
  this.core.on('error', function (err) {
    self.emit('error', err)
  })
  this.index = opts.index

  this.writer = null
  this.readyFns = []
  this.core.writer('default', function (err, writer) {
    if (err) return self.emit('error', err)
    self.writer = writer
    self.readyFns.forEach(function (fn) { fn() })
    self.readyFns = []
  })

  // Create indexes
  var kv = umkv(sub(this.index, 'kvu'))
  this.core.use('kv', 2, createKvIndex(kv, sub(this.index, 'kvi')))
}

EchoDB.prototype = Object.create(EventEmitter.prototype)

// Is the log ready for writing?
EchoDB.prototype._ready = function (cb) {
  if (this.writer) cb()
  else this.readyFns.push(cb)
}

EchoDB.prototype.ready = function (cb) {
  // TODO: one day we'll have a readonly mode!
  if (!this.writer) {
    this.readyFns.push(cb)
    return
  }
  this.core.ready(cb)
}

// EchoDBElement -> Error
EchoDB.prototype.create = function (element, cb) {
  // Generate unique ID for element
  var id = generateId()

  this.put(id, element, cb)
}

// EchoDBId -> [EchoDBElement]
EchoDB.prototype.get = function (id, cb) {
  var self = this

  var elms = []
  var error
  var pending = 0

  this.core.api.kv.get(id, function (err, versions) {
    if (err) return cb(err)
    versions = versions || []
    pending = versions.length + 1

    for (var i = 0; i < versions.length; i++) {
      self.getByVersion(versions[i], done)
    }
    done()
  })

  function done (err, elm) {
    if (err) error = err
    if (elm) elms.push(elm)
    if (--pending) return
    if (error) cb(error)
    else cb(null, elms)
  }
}

// String -> [Message]
EchoDB.prototype._getByVersion = function (version, cb) {
  var key = version.split('@')[0]
  var seq = version.split('@')[1]
  var feed = this.core._logs.feed(key)
  if (feed) {
    feed.get(seq, { wait:false }, cb)
  } else {
    process.nextTick(cb, null, null)
  }
}

// EchoDBVersion -> [EchoDBElement]
EchoDB.prototype.getByVersion = function (version, opts, cb) {
  if (typeof opts === 'function' && !cb) {
    cb = opts
    opts = {}
  }

  this._getByVersion(version, function (err, doc) {
    if (err) return cb(err)
    if (!doc) return cb(null, null)
    doc = Object.assign({}, doc, { version: version })
    cb(null, doc)
  })
}

// EchoDBId, EchoDBElement -> EchoDBElement
EchoDB.prototype.put = function (id, element, opts, cb) {
  if (opts && !cb && typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  opts = opts || {}

  var self = this

  var doc = Object.assign({}, element, { id: id, timestamp: new Date().toISOString() })

  // set links
  if (opts.links) {
    doc.links = opts.links
    write()
  } else {
    self.core.api.kv.get(id, function (err, versions) {
      if (err) return cb(err)
      doc.links = versions
      write()
    })
  }

  // write to the feed
  function write () {
    self._ready(function () {
      self.writer.append(doc, function (err) {
        if (err) return cb(err)
        var version = self.writer.key.toString('hex') +
          '@' + (self.writer.length - 1)
        var elm = Object.assign({}, element, {
          version: version,
          id: id,
          links: doc.links || []
        })
        cb(null, elm)
      })
    })
  }
}

// EchoDBId, EchoDBElement -> EchoDBElement
EchoDB.prototype.del = function (id, element, opts, cb) {
  if (opts && !cb && typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  opts = opts || {}

  var self = this

  getLinks(function (err, links) {
    if (err) return cb(err)
    getElms(links, function (err, elms) {
      if (err) return cb(err)
      if (!elms.length) return cb(new Error('no elements exist with id ' + id))
      var doc = Object.assign({}, element, { id: id, deleted: true, type: elms[0].type, links: links })
      write(doc, cb)
    })
  })

  // Get links
  function getLinks (cb) {
    if (opts.links) {
      cb(null, opts.links)
    } else {
      self.core.api.kv.get(id, function (err, versions) {
        if (err) return cb(err)
        cb(null, versions)
      })
    }
  }

  function getElms (links, cb) {
    if (!links.length) return cb(null, [])

    var res = []
    var error
    var pending = links.length
    for (var i = 0; i < links.length; i++) {
      self.getByVersion(links[i], onElm)
    }

    function onElm (err, elm) {
      if (err) error = err
      else res.push(elm)
      if (--pending) return
      if (error) return cb(error)
      cb(null, res)
    }
  }

  // write to the feed
  function write (doc, cb) {
    self._ready(function () {
      doc.id = id
      self.writer.append(doc, function (err) {
        if (err) return cb(err)
        var version = self.writer.key.toString('hex') +
          '@' + (self.writer.length - 1)
        var elm = Object.assign({}, element, { id: id, version: version })
        cb(null, elm)
      })
    })
  }
}

// TODO: should element validation happen on batch jobs?
EchoDB.prototype.batch = function (ops, cb) {
  if (!ops || !ops.length) return cb()

  var self = this
  cb = once(cb)

  populateMissingLinks(function (err) {
    if (err) return cb(err)
    writeData(cb)
  })

  function populateMissingLinks (cb) {
    var pending = 1
    for (var i = 0; i < ops.length; i++) {
      if (!ops[i].id) {
        ops[i].id = generateId()
        ops[i].value.links = []
      } else if (!ops[i].value.links) {
        pending++
        ;(function get (op) {
          self.core.api.kv.get(op.id, function (err, versions) {
            op.value.links = versions || []
            if (!--pending) cb(err)
          })
        })(ops[i])
      }
    }
    if (!--pending) cb()
  }

  function writeData (cb) {
    var batch = ops.map(opToMsg)

    self._ready(function () {
      var key = self.writer.key.toString('hex')
      var startSeq = self.writer.length
      self.writer.append(batch, function (err) {
        if (err) return cb(err)
        var res = batch.map(function (doc, n) {
          var version = key + '@' + (startSeq + n)
          return Object.assign({}, doc, {
            id: doc.id,
            version: version
          })
        })
        cb(null, res)
      })
    })
  }

  function opToMsg (op) {
    if (op.type === 'put') {
      return Object.assign({}, op.value, { id: op.id })
    } else if (op.type === 'del') {
      return Object.assign({}, op.value, { id: op.id, deleted: true })
    } else {
      cb(new Error('unknown type'))
    }
  }
}

EchoDB.prototype.byType = function (type, opts) {
  opts = opts || {}
  var self = this

  var fetch = through.obj(function (row, _, next) {
    self._getByVersion(row.version, function (err, elm) {
      if (err) return next(err)
      var res = Object.assign(elm, {
        version: row.version,
        id: row.id
      })
      next(null, res)
    })
  })

  var ropts = {}
  if (opts.limit) ropts.limit = opts.limit

  return pumpify.obj(this.core.api.types.createReadStream(type, ropts), fetch)
}

EchoDB.prototype.replicate = function (opts) {
  return this.core.replicate(opts)
}

/*
EchoDB.prototype.history = function (opts) {
  if (opts && opts.id && opts.type) {
    var stream = through.obj()
    var err = new Error('id and type are exclusive history options')
    process.nextTick(function () {
      stream.emit('error', err)
    })
    return stream
  } else if (opts && opts.id) {
    return this.core.api.history.id(opts.id, opts)
  } else if (opts && opts.type) {
    return this.core.api.history.type(opts.type, opts)
  } else {
    return this.core.api.history.all(opts)
  }
}
*/

// generateId :: String
function generateId () {
  return randomBytes(8).toString('hex')
}
