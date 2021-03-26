const settings = require('./settings.js');
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const MinimalMongodb = require('MinimalMongodb');

function alwaysArray(item) {
  if (!item){
    return [];
  }else if (!(item instanceof Array)) {
    return [item]
  }else {
    return item
  }
}

(async function() {
  sql.on('error', err => {
    console.log('sql err:',err.toString());
  });
  const pool = new sql.ConnectionPool(settings.srcDb);
  await pool.connect();
  console.log('pool ready')

  const dbConnector=new MinimalMongodb(settings.dstDb);
  const mdb=await dbConnector.connect();
  console.log(`MongoDb connected db:${settings.dstDb.db}`)


  function copyTableToMongo(table, mongoCollection, idKey = 'RowID', offset = 0, where = '', onDoc) {
    return new Promise((resolve, reject) => {
      const request = new sql.Request(pool);
      request.stream = true // You can set streaming differently for each request
      const q = `select * from ${table} WITH (NOLOCK) ${where} order by [${idKey}] asc Offset ${offset} Rows`;
      console.log(q)
      request.query(q)
      let rowCnt = 0;
      request.on('row', async (row) => {
        rowCnt++;
        request.pause();
        //const oldDoc = await mdb.collection(mongoCollection).findOne({
        //  _id: row[idKey]
        //});

        onDoc && await onDoc(row);

        //(!oldDoc || !oldDoc.SysRowTimestamp || (Buffer.compare(Buffer.from(oldDoc.SysRowTimestamp), row.SysRowTimestamp) !== 0)) &&
        await mdb.collection(mongoCollection).updateOne({
          _id: row[idKey]
        }, {
          $set: row
        }, {
          upsert: true
        })
        request.resume();
        (rowCnt % 100) === 0 && console.log(`copying rows table:${table} rows:${rowCnt}`);
      })

      request.on('error', err => {
        reject(err)
        console.log('err', err)
      })

      request.on('done', result => {
        console.log(`copyTableToMongo ${table} to ${mongoCollection} completed rows:${rowCnt}`);
        resolve();
      })
    });
  }

  //await copyTableToMongo('dvdb.dbo.[dvtable_{FE27631D-EEEA-4E2E-A04C-D4351282FB55}]', 'dvFoldersTree', 'RowID', 0);
  await copyTableToMongo('dvdb.dbo.[dvtable_{DBC8AE9D-C1D2-4D5E-978B-339D22B32482}]', 'dvUsers', 'RowID', 0);
  process.exit(0);
})()
