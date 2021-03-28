const cluster = require('cluster');
const importDv = require('./importDv.js')
const settings = require('./settings.js');

if (cluster.isMaster) {

  // Start workers and listen for messages containing notifyRequest
  const numWorkers = settings.workers || require('os').cpus().length;
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork();
  }
  let working = numWorkers;

  for (const id in cluster.workers) {
    cluster.workers[id].processed = 0;
    cluster.workers[id].ready = false;
    cluster.workers[id].on('message', (msg) => {
      const wrk = cluster.workers[id];

      if (msg.cmd === 'broadcast') {
        for (const idb in cluster.workers) {
          idb !== id && cluster.workers[idb].send(msg)
        }
      }else if (msg.cmd === 'finish') {
        working--;
        if (!working) {
          console.log('Finish main process');
          process.exit(0);
        }
      }
    });
  }

} else {
  importDv().then(async(getNextJob) => {
    let processedTotal = 0;
    let processed = 0
    do {
      processed = await getNextJob();
      processedTotal += processed;
    } while (processed)
    process.send({
      cmd: 'finish'
    });
    process.exit(0);
  })
}
