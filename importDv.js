const settings = require('./settings.js');
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const MinimalMongodb = require('MinimalMongodb');
const PrepareWords = require('PrepareWords');
const parser = require('fast-xml-parser');
const he = require('he');
const async = require('async');
const optimist = require('optimist');

const xmlParseOptions = {
    attributeNamePrefix : "",
    attrNodeName: "attr", //default is 'false'
    textNodeName : "text",
    ignoreAttributes : false,
    ignoreNameSpace : false,
    allowBooleanAttributes : true,
    parseNodeValue : true,
    parseAttributeValue : true,
    trimValues: true,
    cdataTagName: "__cdata", //default is 'false'
    cdataPositionChar: "\\c",
    parseTrueNumberOnly: false,
    arrayMode: false
};

const LocaleId = 1;
function alwaysArray(item) {
  if (!item){
    return [];
  }else if (!(item instanceof Array)) {
    return [item]
  }else {
    return item
  }
}

module.exports = async function() {
  sql.on('error', err => {
    console.log('sql err:',err.toString());
  });
  const pool = new sql.ConnectionPool(settings.srcDb);
  await pool.connect();
  console.log('pool ready')

  const dbConnector=new MinimalMongodb(settings.dstDb);
  const mdb=await dbConnector.connect();
  console.log(`MongoDb connected db:${settings.dstDb.db}`)
  const dvCardsCollection = mdb.collection('dvCards')

  const bucket = new MongoDb.GridFSBucket(mdb, {
    bucketName: 'dvFiles'
  });

  const CardTypes = {}; //empty cache

  async function getCardType(CardTypeID){
    if (CardTypes[CardTypeID]) {
      //cache
      return CardTypes[CardTypeID];
    }
    const cardType = (await getRows('dvsys_carddefs', 'CardTypeID', CardTypeID))[0]
    //if( parser.validate(cardType.XMLSchema) === true) { //optional (it'll return an object in case it's not valid)
    const Schema = parser.parse(cardType.XMLSchema, xmlParseOptions);
    delete cardType.XMLSchema;
    delete cardType.XSDSchema;
    delete cardType.Icon;
    cardType.fields = [];
    vfs = alwaysArray(Schema.CardDefinition?.VirtualFields?.VirtualField)

    vfs.forEach((field) => {
      const vf = (parser.parse(he.decode(field.Data), xmlParseOptions)).VirtualField;
      const tagCParts = vf.ComputedField?.ComputationParts || vf.ComputedField?.ComputationGroup?.ComputationParts
      let compParts = alwaysArray(tagCParts?.ComputationPart)
      compParts = compParts.filter(cpart => (cpart.DataItem && cpart.DataItem.attr.SectionAlias && cpart.DataItem.attr.Value))

      const fields = compParts.map(cpart => `${cpart.DataItem.attr.SectionAlias}.${cpart.DataItem.attr.Value}`)
      const fieldAliases = compParts.map(cpart => `${cpart.DataItem.attr.Value}`)
      if (vf?.SectionField?.attr) {
        fields.push(`${vf.SectionField.attr.SectionAlias}.${vf.SectionField.attr.Name} as ${vf.SectionField.attr.Alias}`)
        fieldAliases.push(vf.SectionField.attr.Alias)
      }
      let fieldsStr = fields.length ? fields.join(', ') : '*';

      if (tagCParts?.Aggregation?.attr?.Function === 'Count') {
        const agrAttrs = tagCParts?.Aggregation?.attr;
        const cntField = (agrAttrs.SectionAlias &&  agrAttrs.PrimaryKey) ? `${agrAttrs.SectionAlias}.${agrAttrs.PrimaryKey}` : fieldsStr
        fieldsStr = `count (${cntField}) as cnt`
      }

      let sqlQ = `select ${fieldsStr} from dvdb.dbo.[dvtable_{${vf.attr.SectionTypeID}}] main WITH (NOLOCK)`
      //console.log('sqlQ', sqlQ)
      //process.exit(3);

      const jds = alwaysArray(vf?.JoinDefs?.JoinDef);
      jds.forEach((jd) => {
        const tblName = jd.attr.TableName ?
          `dvdb.dbo.[${jd.attr.TableName}]`
          : `dvdb.dbo.[dvtable_{${jd.attr.SectionID}}]`;
        sqlQ += ` \n inner join ${tblName} ${jd.attr.Alias} WITH (NOLOCK) on ${jd.attr.SourceAlias}.${jd.attr.SourceField} = ${jd.attr.Alias}.${jd.attr.DestField}`
      })
      sqlQ+= ' where main.InstanceID = @ID'
      cardType.fields.push({
        Name: field.Name.LocalizedString[LocaleId].text,
        sql: sqlQ,
        aggregation: tagCParts?.Aggregation?.attr,
        oneField: fieldAliases.length === 1 ? fieldAliases[0] : false,
        Data: vf
      })
    })
    //}else {
    //  proces.exit(2);
    //}
    cardType.sections = [];
    const sections = {};
    (await sqlRows('select * from dvdb.dbo.[dvsys_sectiondefs] WITH (NOLOCK) where CardTypeID = @ID \
    OR SectionTypeID = cast(\'FE27631D-EEEA-4E2E-A04C-D4351282FB55\' as uniqueidentifier)\
', CardTypeID)).forEach((sectionDef) => {
      sections[sectionDef.SectionTypeID] = sectionDef;
    })
    for (let sectionId in sections) {
      let sectionDef = sections[sectionId]
      if (sectionDef.ParentSectionID === '00000000-0000-0000-0000-000000000000'){
        cardType.sections.push(sectionDef);
      } else {
        sections[sectionDef.ParentSectionID].sections = sections[sectionDef.ParentSectionID].sections || [];
        sections[sectionDef.ParentSectionID].sections.push(sectionDef)
      }
      sectionDef.columns = (await sqlRows(`select Alias,LinkType from dvdb.dbo.[dvsys_fielddefs] WITH (NOLOCK) where [SectionTypeID] = @ID`, sectionId));
    }
    CardTypes.lastChange = (new Date()).getTime();
    CardTypes[CardTypeID] = cardType;
    process.send && process.send({
      cmd: 'broadcast',
      type: 'setCardType',
      CardTypeID: CardTypeID,
      CardType: cardType
    });
    //await saveCardTypes();
    return cardType;
  }

  process.on('message', (msg) => {
    if (msg.type === 'setCardType') {
      CardTypes[msg.CardTypeID] = msg.CardType
    }
  });

  function asyncSql(query, onDoc){
    return new Promise((resolve, reject) => {
      const request = new sql.Request(pool);
      request.stream = true // You can set streaming differently for each request
      request.query(query);
      request.on('row', async (row) => {
        request.pause();
        onDoc && await onDoc(row);
        request.resume();
      });
      request.on('error', reject);
      request.on('done', resolve);
    })
  }

  async function extendFields(fields, InstanceID) {
    const ret={}
    for (let field of fields){
      try{
        ret[field.Name] = await sqlRows(field.sql, InstanceID)
      }catch(e){
        console.log(`Error in request ${field.sql} ID:${InstanceID}`)
        throw e;
      }

      if (field?.aggregation?.Function === 'Count') {
        ret[field.Name] = ret[field.Name][0].cnt;
      }else if (field.oneField) {
        ret[field.Name] = ret[field.Name].map(a => a[field.oneField])
      }
    }
    return ret;
  }
  async function extendProps(sections, InstanceID) {
    const ret = {}
    for (let section of sections) {
      const colNames = [ 'RowID', 'ParentRowID' ].concat(section.columns.map(a => `[${a.Alias}]`));

      let rows = await sqlRows(`select ${colNames.join(', ')} from dvdb.dbo.[dvtable_\{${section.SectionTypeID}\}] \
        WITH (NOLOCK) where InstanceID = @ID`, InstanceID);

      ret[section.Alias] = rows.length === 1 ? rows[0] : rows;

      if (section.sections && section.sections.length) {
        ret[section.Alias+'_'] = await extendProps(section.sections, InstanceID)
      }
    }
    return ret;
  }

  async function getRows(table, idKey, id) {
    const req = pool.request();
    req.input('ID', sql.UniqueIdentifier, id);

    do {
      try{
        const rows = (await req.query(`select * from dvdb.dbo.[${table}] WITH (NOLOCK) where [${idKey}] = @ID`)).recordset;
        return rows;
      }catch(e){
        if (e.code !== 'ETIMEOUT'){
          throw e;
        }else {
          console.log('Timeout query, try again in 5 sec')
          await timeout(5000)
        }
      }
    } while ( true )
  }

  function timeout(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

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

  async function fillFolders(doc) {
    if (!doc.FolderRowId) return;
    const r = await mdb.collection('dvFoldersTree').aggregate([{
        $match: {
          RowID: doc.FolderRowId
        }
      },
      {
        $graphLookup: {
          from: 'dvFoldersTree',
          startWith: '$ParentTreeRowID',
          connectFromField: 'ParentTreeRowID',
          connectToField: '_id',
          as: 'folders',
          //restrictSearchWithMatch: { Deleted: null },
          depthField: 'level'
        }
      }
    ]).toArray();
    if (!r[0]) {
      throw new Error("Folders request failed")
    }
    const folders = r[0].folders;
    delete r[0].folders;
    r[0].level = -1;
    folders.unshift(r[0])
    folders.sort((a ,b) => {
      if (a.level > b.level) return -1;
      if (a.level > b.level) return 1;
    })
    doc.folders = folders;
    doc.strFolders = folders.map(a => a.Name).join('>')
  }

  async function asyncEndStream(stream, chunk) {
    return new Promise((resolve, reject) => {
      stream.on('error', reject);
      stream.end(chunk, resolve);
    });
  }

  async function extendInstance(child, path, rootDoc, CardDocs) {
    await fillFolders(child);
    const ID = child.InstanceID;
    if (rootDoc.InstanceIDs.includes(ID)) {
      child.LOOP='LOOP';
      rootDoc.loopInstanceId = rootDoc.loopInstanceId || [];
      rootDoc.loopInstanceId.push(ID+' >> '+ path);
      return;
    }
    rootDoc.InstanceIDs.push(ID);
    const cardType = await getCardType(child.CardTypeID);
    child.CardTypeAlias = cardType.Alias;

    child.instanceDate = (await getRows('dvsys_instances_date', 'InstanceID', ID))[0];
    //await extendRefs(child);

    child.links = await getRows('dvtable_{CD2746F7-2DBD-4D72-8F70-3B667B9409A7}', 'Link', ID);
    child.sections = await extendProps(cardType.sections, ID);
    child.fields = await extendFields(cardType.fields, ID);
    //child.debug = { sections: cardType.sections };
    const childs = await sqlRows('select dvCards.ParentRowID as FolderRowId, instanceTbl.*\
    from dvdb.dbo.[dvsys_instances] instanceTbl WITH (NOLOCK)\
    left join dvdb.dbo.[dvtable_{EB1D77DD-45BD-4A5E-82A7-A0E3B1EB1D74}] dvCards WITH (NOLOCK) on instanceTbl.InstanceID = dvCards.HardCardID\
    where instanceTbl.ParentID = @ID', ID);
    child.path = path;
    for (let subChild of childs){
      await extendInstance(subChild, `${path}>${subChild.InstanceID}`, rootDoc, CardDocs);
    }

    child.rootCardId = rootDoc.InstanceID;
    child._id = child.InstanceID;

    if (child.sections.MainInfo.FileID) {
      child.binaryFileInfo = (await sqlRows('select top 1 dvFiles.Name, dvFiles.BinaryID, ext1.version from dvdb.dbo.dvsys_files dvFiles WITH (NOLOCK)\
  inner join dvdb.dbo.[dvtable_{F831372E-8A76-4ABC-AF15-D86DC5FFBE12}] ext1 WITH (NOLOCK) on ext1.FileID = dvFiles.FileID \
  where dvFiles.OwnerCardID = @ID\
  ORDER by ext1.version desc', child.sections.MainInfo.FileID))[0];

      const ff=(await sqlRows(`select * from dvdb.dbo.dvsys_binaries WITH (NOLOCK) where ID = @ID`, child.binaryFileInfo.BinaryID))[0];
      const binId = ff.ID;
      const bindata = ff.Data || ff.StreamData;

      child.binaryFileInfo.size = bindata.length;

      try {
        await asyncEndStream(bucket.openUploadStreamWithId(binId, child.binaryFileInfo.Name, {
          metadata: child.binaryFileInfo
        }), bindata);
        console.log('file saved', child.binaryFileInfo.Name)
        //await mdb.collection('dvBinary').insertOne(ff);
      } catch(e) {
        if (e.code === 11000) {
          console.log('e', e.toString());
          const oldFileDoc = (await (bucket.find({ _id: binId }).toArray()))?.[0];

          if (oldFileDoc && (oldFileDoc.length===bindata.length)){
            console.log('file size same, skip upload', binId, oldFileDoc.length , bindata.length);
          } else {
            console.log('different file size', binId, oldFileDoc?.length || 0 , bindata.length);
            console.log('rewrite file')
            await (new Promise((resolve, reject) => {
              bucket.delete(binId, async() => {
                console.log('file removed')
                await asyncEndStream(bucket.openUploadStreamWithId(binId, child.binaryFileInfo.Name, {
                  metadata: child.binaryFileInfo
                }), bindata);
                console.log('file saved', child.binaryFileInfo.Name)
                resolve();
              });
            }))

          }
        } else {
          throw e;
        }
      }

      //console.log('ff', ff)
    }

    delete child.InstanceID;
    CardDocs.push(child);
  } //extendInstance

  async function saveCardDocs(CardDocs){
    try{
      await dvCardsCollection.insertMany(CardDocs);
      console.log(`dvCards inserted cnt:${CardDocs.length}`);
    }catch(e){
      if (e.code !== 11000) {
        throw e;
      }
      let updCnt = 0;
      while(CardDocs.length) {
        let doc = CardDocs.shift();
        updCnt++;
        await dvCardsCollection.updateOne({
          _id: doc._id
        }, {
          $set: doc
        }, {
          upsert: true
        });
      }
      console.log(`dvCards updated cnt:${updCnt}`);
      //await dvCardsCollection.removeMany({ _id: {$in: CardDocs.map(a => a._id)} })
      //await dvCardsCollection.insertMany(CardDocs);
    }
  }
  await dvCardsCollection.createIndex({ 'ParentID':1 });
  await dvCardsCollection.createIndex({ 'CreationDateTime':1 });

  async function processRootCard(doc, CardDocs) {
    doc.InstanceIDs = [];
    await extendInstance(doc, `root:${doc.InstanceID}`, doc, CardDocs);

    console.log(`rDoc folder:${doc.strFolders} Description:${doc.Description} id:${doc._id}`); //ids:${CardDocs.map(a => a._id).join(', ')}`)
    return 'good';
  }

  if (optimist.argv.search) {
    return {
      processRootCard,
      saveCardDocs,
      asyncSql,
      mdb
    }
  }

  //const parallelJobs = (settings.parallel || 10)
  async function getNextJob() {
    //console.log('getNextJob', lastCardI)
    const qResult = await mdb.collection('dvCardsQueued').findOneAndDelete({})
    if (!qResult.value){
      console.log('dvCardsQueued empty. Quit.')
      return 0;
    }


    const ssql = `select TOP 1 dvDates.CreationDateTime, dvCards.ParentRowID as FolderRowId, instanceTbl.*\
    from dvdb.dbo.[dvtable_{EB1D77DD-45BD-4A5E-82A7-A0E3B1EB1D74}] dvCards WITH (NOLOCK)\
    inner join dvdb.dbo.[dvsys_instances] instanceTbl WITH (NOLOCK) on instanceTbl.InstanceID = dvCards.HardCardID\
    inner join dvdb.dbo.[dvsys_instances_date] dvDates WITH (NOLOCK) on instanceTbl.InstanceID = dvDates.InstanceID\
    where dvCards.HardCardID= @ID`;
    //console.log(ssql);
    const jobs = await sqlRows(ssql, qResult.value._id);

    const processed = jobs.length;

    if (!jobs.length) {
      process.exit(0)
    }

    const CardDocs = [];
    await processRootCard(jobs[0], CardDocs);
    await saveCardDocs(CardDocs);

    //console.log(`job results:${results.join(', ')}`)

    return processed;
  }
  if (optimist.argv.singleJob) {
    await getNextJob();
  }

  return getNextJob;

/*
  'dvtable_{F06A18E7-582E-4896-9C0C-146025E6D9DA}': { InstanceID: 1 }, --> ApprovalID ??
  'dvtable_{55EF9765-2651-4F13-A716-4606B729881C}': { InstanceID: 1 }, -->SelectedValue ??
  'dvtable_{47C41171-9C64-450A-A3A6-102B3156AD79}': { InstanceID: 2 }, --> Люди и их должность и положение в структуре
*/
}
optimist.argv.singleJob && module.exports().then(() => {
  console.log('Finish single job test');
  process.exit(0);
})

optimist.argv.help && module.exports().then(() => {
  console.log('--singleJob Process one card')
  console.log('--search Create dvCardsQueued collection')
  process.exit(0);
})

optimist.argv.search && module.exports().then(async(functions) => {
  const { processRootCard, saveCardDocs, asyncSql, mdb } = functions;
  const ssql = `select dvDates.CreationDateTime, dvCards.ParentRowID as FolderRowId, instanceTbl.*\
  from dvdb.dbo.[dvtable_{EB1D77DD-45BD-4A5E-82A7-A0E3B1EB1D74}] dvCards WITH (NOLOCK)\
  inner join dvdb.dbo.[dvsys_instances] instanceTbl WITH (NOLOCK) on instanceTbl.InstanceID = dvCards.HardCardID\
  inner join dvdb.dbo.[dvsys_instances_date] dvDates WITH (NOLOCK) on instanceTbl.InstanceID = dvDates.InstanceID\
  where instanceTbl.ParentID = cast(\'00000000-0000-0000-0000-000000000000\' as uniqueidentifier)`;

  let cnt = 0;
  console.log(ssql);
  setInterval(()=> {
    console.log(`Processed:${cnt}`)
  }, 10000).unref()
  await asyncSql(ssql, async (doc) => {
    cnt++;
    const adoc = await mdb.collection('dvCards').findOne({
      _id: doc.InstanceID
    }, {
      projection: { _id:1 }
    });

    if (adoc) {
      return
    }

    console.log('Missing card!', doc.InstanceID);

    await mdb.collection('dvCardsQueued').updateOne({
      _id: doc.InstanceID
    }, {
      $set: {
        status: 1
      }
    }, {
      upsert: true
    })
    //const Cards = [];
    //await processRootCard(doc, Cards);
    //await saveCardDocs(Cards);
  })
  console.log('Finish')
  process.exit(0);
})
