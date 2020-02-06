const assert = require('assert')
const fs = require('fs')
const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const Binary = mongodb.Binary

const url = 'mongodb://mongo:27017'

// Database Name
const dbName = 'dhtspider'

let collection
// Use connect method to connect to the server
MongoClient.connect(url, { useUnifiedTopology: true }, function(err, client) {
  assert.equal(null, err)
  console.log('Connected successfully to server')

  const db = client.db(dbName)
  collection = db.collection('hash')
})

const fifoPath = '/var/dhtspider/fifo'
const fifo = fs.createReadStream(fifoPath, 'utf8')

let hex = ''
let hexLen = 40
let len = 0
fifo.on('data', data => {
  for (let i = 0; i < data.length; i++) {
    len++
    if (len === hexLen + 1) {
      if (data[i] !== '\n') {
        throw Error('data[i] not \n')
      }
      process(hex)
      hex = ''
      len = 0
    } else {
      hex += data[i]
    }
  }
})

console.log('dhtspider-db started!')

function process(hex) {
  let tmpBuf = Buffer.from(hex, 'hex')
  let base64 = tmpBuf.toString('base64')

  collection.updateOne(
    {
      _id: new Binary(tmpBuf, '00')
    },
    {
      $inc: { count: 1 }
    },
    {
      upsert: true
    },
    function(err, result) {
      if (err) {
        console.log(err)
      }

      assert.equal(1, result.result.n)
    }
  )
}
