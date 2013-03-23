module.exports = create_odb

create_odb.accept = accept

var path = require('path')
  , idxparse = require('git-packidx-parser')
  , packfile = require('git-packfile')
  , extrex = /\.idx$/

function create_odb(entry, fs, find_oid, ready) {
  var source = entry.path
    , pack_path = source.replace(extrex, '.pack')
    , stat = entry.stat
    , packstat
    , pack_idx
    , pack

  fs.stat(pack_path, function(err, _packstat) {
    if(err) {
      return ready(err)
    }

    packstat = _packstat

    fs.createReadStream(source)
      .pipe(idxparse())
      .on('data', gotidx)
      .on('error', ready)
    
  })

  function gotidx(idx) {
    pack_idx = idx
    pack = packfile(packstat.size, find_oid, readpack)

    return ready(null, {
        readable: true
      , writable: false
      , read: pack_read
      , write: noop_write
    })
  }

  function readpack(start, end) {
    return fs.createReadStream(pack_path, {
      start: start
    , end: end
    })
  }

  function pack_read(oid, ready) {
    var location = pack_idx.find(oid)

    if(!location) {
      return ready(null, undefined)
    }

    pack.read(location.offset, location.next, ready)
  }
}

function accept(path) {
  return /objects\/pack\/.*\.idx$/.test(path) 
}

function noop_write(oid, blob, ready) {

}
