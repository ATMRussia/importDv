const settings = require('./settings.js');
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const optimist = require('optimist');
const MinimalMongodb = require('MinimalMongodb');
const PrepareWords = require('PrepareWords');

if (!optimist.argv.id) {
  console.log('Find ID in database');
  console.log('--id <GUID>');
  process.exit(1);
}

(async function() {
  const pool = await sql.connect(settings.srcDb)
  console.log('pool ready')

  const request = new sql.Request(pool);
  request.stream = true // You can set streaming differently for each request
  request.query('use dvdb; SELECT * FROM information_schema.columns \
 WITH (NOLOCK) \
 where\
 DATA_TYPE like \'uniqueidentifier\' AND \
 TABLE_NAME not like \'%view%\' AND TABLE_NAME not like \'%archive%\' \
 AND \
 TABLE_CATALOG like \'dvdb\' AND TABLE_SCHEMA like \'dbo\'')

  const result = {};

  request.on('row', async (column) => {
    request.pause();

    //console.log(`try find in tbl:${column.TABLE_NAME} col:${column.COLUMN_NAME}`)
    // DA86FABF-4DD7-4A86-B6FF-C58C24D12DE2 InstanceID Информационный отдел
    // RowID BFED1042-8CAA-4F5E-86E9-A0CA96A5F72D Информационный отдел
    const srch = optimist.argv.id;
    const q = `select count(*) as cnt from dvdb.dbo.[${column.TABLE_NAME}] WITH (NOLOCK) where [${column.COLUMN_NAME}] = cast('${srch}' as uniqueidentifier)`;
    console.log(`q:${q}`)
    try {
      const rows = (await pool.request().query(q)).recordset;
      if (rows[0].cnt) {
        console.log(`found in tbl:${column.TABLE_NAME} col:${column.COLUMN_NAME}`)
        result[column.TABLE_NAME] = result[column.TABLE_NAME] || {}
        result[column.TABLE_NAME][column.COLUMN_NAME] = rows[0].cnt
      }
    }catch(e){
      console.log('Err:', e.toString())
    }

    request.resume();
  })

  request.on('error', err => {
    throw err;
  })

  request.on('done', rrr => {
    console.log(`Done!`)
    console.dir(result);
  })

})().then(() => {
  console.log('app finish')
}).catch((err) => {
  console.log('app crushed with error')
  console.log(err)
});

sql.on('error', err => {
  console.log('sql err:',err.toString());
});
