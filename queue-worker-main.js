import QueueWorker from './queue-worker.js';

const queueWorker = new QueueWorker();

try {
    await queueWorker.run();
}
catch (err) {
    console.error(err);

    queueWorker.stop();
}

