const settings = require('./settings.js');
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const MinimalMongodb = require('MinimalMongodb');
const async = require('async');
const optimist = require('optimist');

async function asyncEndStream(stream, chunk) {
  return new Promise((resolve, reject) => {
    stream.on('error', reject);
    stream.end(chunk, resolve);
  });
}

async function start() {
  sql.on('error', err => {
    console.log('sql err:',err.toString());
  });
  const pool = new sql.ConnectionPool(settings.srcDb);
  await pool.connect();
  console.log('pool ready')

  async function sqlRows(sqlT, id, Vdate, id1) {
    const req = pool.request();
    id && req.input('ID', sql.UniqueIdentifier, id);
    Vdate && req.input('Vdate', sql.DateTime, Vdate);
    id1 && req.input('ID1', sql.UniqueIdentifier, id1);
    do {
      try{
        const rows = (await req.query(sqlT)).recordset;
        return rows;
      }catch(e){
        if (e.code !== 'ETIMEOUT'){
          throw e;
        }else {
          console.log('Timeout query, try again in 5 sec')
          await timeout(5000)
        }
      }
    }while ( true )
  }

  const dbConnector=new MinimalMongodb(settings.dstDb);
  const mdb=await dbConnector.connect();
  console.log(`MongoDb connected db:${settings.dstDb.db}`)
  const dvCardsCollection = mdb.collection('dvCards')

  const bucket = new MongoDb.GridFSBucket(mdb, {
    bucketName: 'dvFiles'
  });

  const cardsCursor = mdb.collection('dvCards').aggregate([
    {
      $match: {
        'binaryFileInfo.BinaryID': { $ne: null },
        //missingFile: true
      }
    },
    { $lookup: {
      from: 'dvFiles.files',
      localField: 'binaryFileInfo.BinaryID',
      foreignField: '_id',
      as: 'fileDoc'
    } },
    { $match: { fileDoc: { $size: 0 } } },
    { $project: { binaryFileInfo:1 } }
  ]);

  let missingCnt = 0;
  for await (let card of cardsCursor) {
    console.log(`bad card ${card._id} BinaryID:${card.binaryFileInfo.BinaryID}`)
    let docExt = await mdb.collection('dvFiles.files').findOne({ _id: card.binaryFileInfo.BinaryID }, {
      projection: { _id:1 }
    })
    if (docExt) {
      continue;
    }
    const ff=(await sqlRows(`select * from dvdb.dbo.dvsys_binaries WITH (NOLOCK) where ID = @ID`, card.binaryFileInfo.BinaryID))[0];
    if (!ff) {
      missingCnt++;
      await mdb.collection('dvCards').updateOne({_id: card._id}, {
        $set:{
          missingFile: true
        }
      });
      continue;
    }
    const binId = ff.ID;
    const bindata = ff.Data || ff.StreamData;
    if (card.binaryFileInfo.size !== bindata.length) {
      throw new Error(`Rather size ${card.binaryFileInfo.size} != ${bindata.length}`)
    }
    try {
      card.binaryFileInfo.size = bindata.length
      await asyncEndStream(bucket.openUploadStreamWithId(binId, card.binaryFileInfo.Name, {
        metadata: card.binaryFileInfo
      }), bindata);
      console.log('file saved', card.binaryFileInfo.Name)
    }catch (e){
      e.code !== 11000 && console.log('Err', e)
    }
  }
  console.log('total missingFile', missingCnt)
}

start().then(() => {
  console.log('finish app')
  process.exit(0);
}).catch((err) => {
  console.log('Exception', err)
  process.exit(10);
})
