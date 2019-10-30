var tape = require('tape')
var kappa = require('kappa-core')
var ram = require('random-access-memory')
var memdb = require('memdb')
var EchoDB = require('./')

var db = EchoDB({
  core: kappa(ram, { valueEncoding: 'json' }),
  index: memdb(),
  storage: function (name, cb) { cb(null, ram()) }
})

tape('testing', (t) => {
  var card = {
    type: 'card',
    title: 'project-1',
    priority: 1,
    complete: false,
    text: 'hey'
  }

  db.create(card, (err, item) => {
    t.error(err)
    console.log('created item', item)
    var mutation = {
      priority: 3,
      complete: true
    }
    db.put(item.id, mutation, {
      links: [item.version]
    }, function done (err, newItem) {
      t.error(err)
      console.log('edited item', newItem)
      db.get(newItem.id, (err, got) => {
        t.error(err)
        t.same(got[0].id, item.id)
        console.log('got item with id', got)
        t.end()
      })
    })
  })
})
