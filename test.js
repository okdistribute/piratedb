var tape = require('tape')
var pump = require('pump')
var kappa = require('kappa-core')
var ram = require('random-access-memory')
var memdb = require('memdb')
var EchoDB = require('./')

var card = {
  type: 'card',
  title: 'project-1',
  priority: 1,
  complete: false,
  text: 'write example code for rich'
}

var card2 = {
  type: 'card',
  title: 'project-2',
  priority: 5,
  complete: false,
  text: 'eat more vegetables'
}

function createDbs () {
  var db = EchoDB({
    core: kappa(ram, { valueEncoding: 'json' }),
    index: memdb(),
    storage: function (name, cb) { cb(null, ram()) }
  })

  var db2 = EchoDB({
    core: kappa(ram, { valueEncoding: 'json' }),
    index: memdb(),
    storage: function (name, cb) { cb(null, ram()) }
  })
  return {a: db, b: db2}
}

tape('update card with single writer', (t) => {
  var dbs = createDbs()

  dbs.a.create(card, (err, item) => {
    t.error(err)
    console.log('created item', item)
    var mutation = Object.assign({}, card, {
      priority: 3,
      complete: true
    })
    dbs.a.put(item.id, mutation, {
      links: [item.version]
    }, function done (err, newItem) {
      t.error(err)
      dbs.a.get(newItem.id, (err, got) => {
        t.error(err)
        var newest = got[0]
        t.same(newest.id, item.id)
        t.same(newest.priority, 3)
        t.same(newest.complete, true)
        t.end()
      })
    })
  })
})

tape('update multiple cards with two writers', (t) => {
  var dbs = createDbs()
  var itema, itemb
  dbs.a.create(card, (err, item) => {
    t.error(err)
    itema = item
    dbs.b.create(card2, (err, item) => {
      t.error(err)
      itemb = item
      replicate(dbs.a, dbs.b, (err) => {
        t.error(err)
        mutate()
      })
    })
  })

  function mutate () {
    var mutation2 = Object.assign({}, card2, {
      text: 'eat more salad'
    })

    dbs.b.put(itemb.id, mutation2, {
      links: [itemb.version]
    }, function done (err, newItem) {
      t.error(err)
      dbs.b.get(newItem.id, (err, got) => {
        t.error(err)
        var mutated = got[0]
        console.log(got)
        t.same(mutated.text, 'eat more salad', 'b is eat more salad')
        dbs.a.get(newItem.id, (err, got) => {
          t.error(err)
          var mutated = got[0]
          console.log(got)
          t.same(mutated.text, 'eat more vegetables', 'a is eat more vegetables')
          replicate(dbs.a, dbs.b, (err) => {
            t.error(err)
            dbs.a.get(mutated.id, (err, replicated) => {
              t.error(err)
              t.same(replicated[0].text, 'eat more salad', 'after replication, a is eat more salad')
              t.end()
            })
          })
        })
      })
    })
  }
})


function replicate (a, b, cb) {
  var stream = a.replicate()
  pump(stream, b.replicate(), stream, cb)
}
