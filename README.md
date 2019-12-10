# ECHO

Eventually Consistent Hierarchical Objects.

The future of P2P replication.

## Details

Someone should be able to implement [`kappa-osm`](https://github.com/digidem/kappa-osm) and `cabal-core` using this
module (but we won't do that).

## API

#### `.create(element, cb)`
  - callback returns item with .version and .id properties
#### `.put(id, data, opts, cb)` 
  - edit an existing item
  - opts contains 'links' Array of versions to link back to old versions of
  this item
  - callback returns new item with updated .version 
#### `.get(opts, cb)`
  - callback returns items that matches given options
  - e.g., {id: ...} or {version: ...}
  
#### `.del(id, cb)`
  - deletes item at id which sets {deleted: true} property
#### `.replicate(opts)`
  - creates replication stream to hand off to broadcast
#### `.use(kappaview)`
  - create a custom kappa-view


