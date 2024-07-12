import { QueuePro, WorkerPro } from '@taskforcesh/bullmq-pro';
import { Worker as WorkerThread, isMainThread, workerData, parentPort } from 'node:worker_threads';
import makeBarrier from '@strong-roots-capital/barrier';

interface Options {
  writers: number;
  readers: number;
  duration: number;
  concurrency: number;
  groups: number;
  hostname: string;
  port: number;
  numJobs: number;
}

function sleep(seconds: number) {
  return new Promise(resolve => setTimeout(resolve, seconds * 1000));
}

async function WriterMain(options: Options) {
  const queue = new QueuePro(
      `{grouped_queue}`,
      {
        connection: {host: options.hostname, port: options.port },
        prefix: 'b',
      });

  const groups = [];
  const groupSequences = {} as Record<string, number>;

  for (let i = 0; i < options.groups; ++i) {
    groups.push("g" + i);
  }

  let barrier = makeBarrier(1);
  if (!parentPort) {
    throw new Error('parentPort is null')
  }

  parentPort.once('message', (msg) => {
    barrier();
  });

  parentPort.postMessage('ready');
  await barrier();

  // Wait in chunks to avoid blocking the event loop
  const chunkSize = 2000;
  const groupChunkSize = 1000;

  let queued = 0;

  while (queued < options.numJobs) {
    for (let i = 0; i < groups.length; i += groupChunkSize) {
      const groupChunk = groups.slice(i, i + groupChunkSize);
      const adding = [];

      for (const group of groupChunk) {
        const seq = groupSequences[group] || 0;
        adding.push(queue.add('job name', { param1: 'value1', seq }, { group: { id: group } }));
        groupSequences[group] = seq + 1;
        queued++;

        if (adding.length >= chunkSize) {
          await Promise.all(adding);
          adding.length = 0;
        }

        if (queued >= options.numJobs) break;
      }

      if (adding.length > 0) {
        await Promise.all(adding);
      }

      if (queued >= options.numJobs) break;
    }
  }

  await queue.close();
  parentPort.postMessage(queued);
}

async function ReaderMain(index: number, targetJobsNum: number, options: Options) {
  let read = 0;
  let isClosing= false;

  let barrier = makeBarrier();
  const worker = new WorkerPro(
    `{grouped_queue}`,
    async job => {
      if (isClosing) {
        // Since we are closing we should not count this job
        // as time has already expired.
        return;
      }
      // await sleep(Math.random() * 0.010);
      if (job.opts.group?.id === 'g0') {
        console.log('t:', index, ':', job.opts.group?.id, ':', job.data.seq);
      }
      if (++read >= targetJobsNum) barrier();
    },
    {
      connection: {host: options.hostname, port: options.port},
      group: {
        concurrency: 1 // Limit to max 1 parallel jobs per group
      },
      concurrency: options.concurrency,
      prefix: 'b',
      removeOnComplete: {count: 0},
      removeOnFail: {count: 0},
    }
  )
  await barrier();

  isClosing = true;
  await worker.close();
  if (!parentPort) {
    throw new Error('parentPort is null');
  }
  parentPort.postMessage(read);
}

function NicifyNumber(n: number) {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function PrintNumber(n: number, seconds: number) {
  return `${NicifyNumber(n)} (${NicifyNumber(Math.round(n / seconds))}/s)`;
}

async function main() {
  const commandLineArgs = require('command-line-args');

  const optionDefinitions = [
    { name: 'writers', alias: 'w', type: Number, defaultValue: 1 },
    { name: 'readers', alias: 'r', type: Number, defaultValue: 1 },
    { name: 'duration', alias: 'd', type: Number, defaultValue: 1 },
    { name: 'numJobs', alias: 'n', type: Number, defaultValue: 500_000 },
    { name: 'groups', alias: 'g', type: Number, defaultValue: 1 },
    { name: 'concurrency', alias: 'c', type: Number, defaultValue: 1 },
    { name: 'hostname', alias: 'h', type: String, defaultValue: 'localhost' },
    { name: 'port', alias: 'p', type: Number, defaultValue: 6379 },
  ];
  const options: Options = commandLineArgs(optionDefinitions);

  console.log("Running with options:", options);

  let barrier = makeBarrier(options.writers);

  console.log(`Initializing ${options.writers} writers`);
  let writes: number[] = [];
  let writers = [];
  let start = process.hrtime();

  for (let i = 0; i < options.writers; ++i) {
    let worker = new WorkerThread(__filename, { workerData: { type: 'writer', options } });
    writers.push(worker);
    worker.once('message', (value) => {
      barrier();
    });
  }
  await barrier();

  barrier = makeBarrier(options.writers);
  for (let writer of writers) {
    writer.once('message', (value) => {
      writes.push(value);
      barrier();
    });
  }

  for (let writer of writers) {
    writer.postMessage("start");
  }

  await barrier();
  let diff = process.hrtime(start);
  const durW = diff[0] * 1e3 + diff[1] * 1e-6;
  console.log(`Writers finished in ${durW.toFixed(0)}ms`);

  barrier = makeBarrier(options.readers);
  const readers = options.readers;
  console.log(`Initializing ${readers} readers`);
  let reads: number[] = [];

  start = process.hrtime();

  for (let i = 0; i < readers; ++i) {
    let worker = new WorkerThread(__filename, { workerData: { type: 'reader', index: i, target: options.numJobs / readers, options } });
    worker.once('message', (value) => {
      reads.push(value);
      barrier();
    });
  }

  await barrier();

  diff = process.hrtime(start);
  const durR = diff[0] * 1e3 + diff[1] * 1e-6;
  console.log(`Readers finished in ${durR.toFixed(0)}ms`);

  const total_writes = writes.reduce((sum, a) => sum += a, 0);
  console.log(`Total writes: ${PrintNumber(total_writes, durW / 1000)}`);
  const total_reads = reads.reduce((sum, a) => sum += a, 0);
  console.log(`Total reads: ${PrintNumber(total_reads, durR / 1000)}`);
}

if (isMainThread) {
  main().catch(console.error);
} else {
  switch (workerData.type) {
    case 'writer':
      WriterMain(workerData.options).catch(console.error);
      break;
    case 'reader':
      ReaderMain(workerData.index, workerData.target, workerData.options).catch(console.error);
      break;
    default:
      throw new Error(`Unknown type ${workerData.type}`);
  }
}