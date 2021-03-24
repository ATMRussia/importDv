const cluster = require('cluster');
const importDv = require('./importDv.js')
const settings = require('./settings.js');

if (cluster.isMaster) {

  // Start workers and listen for messages containing notifyRequest
  const numCPUs = settings.workers || require('os').cpus().length;
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  let lastCard = null;
  let locked = false;
  let stopAll = false;

  function srchReadyWorker(){
    if (locked) return;
    if (stopAll) return;
    for (const idb in cluster.workers) {
      if (cluster.workers[idb].ready){
        //console.log(`=== Send getNextJob worker[${idb}] gt Timestamp:${lastCard && lastCard.CreationDateTime}===`)
        locked = true;
        cluster.workers[idb].ready = false;
        cluster.workers[idb].send({
          cmd: 'getNextJob',
          lastCard: lastCard
        })
        return;
      }
    }
  }

  const statisticInterval = setInterval(() => {
    console.log('-------')
    console.log(`Lock state: ${locked? 'locked': 'free'} StopAll:${stopAll?'Y':'N'}`);
    for (const id in cluster.workers) {
      console.log(`Worker[${id}] ${cluster.workers[id].ready ? 'ready': 'busy'} processed:${cluster.workers[id].processed}`)
    }
    console.log('lastCard', lastCard)
  }, 5000)

  for (const id in cluster.workers) {
    cluster.workers[id].processed = 0;
    cluster.workers[id].ready = false;
    cluster.workers[id].on('message', (msg) => {
      const wrk = cluster.workers[id];
      if (msg.cmd === 'stopAll') {
        stopAll = true;
        clearInterval(statisticInterval);
      }else if (msg.cmd === 'setLastCard') {
        //console.log(`=== Receive setLastCard worker[${id}] ===`, msg.lastCard)
        lastCard = msg.lastCard;
        locked = false;
        srchReadyWorker();
      }else if (msg.cmd === 'ready') {
        wrk.ready = true;
        wrk.processed += msg.processed;
        srchReadyWorker();
      }else if (msg.cmd === 'broadcast') {
        for (const idb in cluster.workers) {
          idb !== id && cluster.workers[idb].send(msg)
        }
      }
    });
  }

} else {
  importDv().then((getNextJob) => {
    process.send({ cmd: 'ready', processed: 0 });
    process.on('message', async (msg) => {
      if (msg.cmd === 'getNextJob') {
        try {
          const processed = await getNextJob(msg.lastCard);
          process.send({
            cmd: 'ready',
            processed: processed
          });
        }catch(e) {
          process.send({ cmd: 'stopAll' });
          console.log('Error', e)
        }

      }
    });
  })

}
