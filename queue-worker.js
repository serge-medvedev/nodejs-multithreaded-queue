import _ from 'lodash';
import { Worker, parentPort } from 'node:worker_threads';
import { Reply } from "zeromq"
import { randomInt } from 'node:crypto';

export default class QueueWorker {
    constructor() {
        this.socket = new Reply({ receiveHighWaterMark: 1 });
    }

    async run() {
        await this.socket.connect('inproc://queue');

        parentPort.postMessage('ready');

        console.log(`worker is ready`);

        for await (const [res] of this.socket) {
            const task = JSON.parse(res);
            const words = _
                .chain(task)
                .get('text')
                .words()
                .size()
                .value();
            const fakeProcessingTime = randomInt(200, 2000);

            setTimeout(async () => {
                console.log(`task: "${task.text}" (${words} words)`);

                const ok = words < 10;

                if (ok) {
                    await this.socket.send(Buffer.from(JSON.stringify({
                        words,
                        uuid: task.uuid
                    })));
                }
                else {
                    this.stop();
                }
            }, fakeProcessingTime);
        }
    }

    stop() {
        this.socket.close();

        process.exit(1);
    }

    static spawn() {
        return new Promise((resolve) => {
            const worker = new Worker('./queue-worker-main.js');

            worker.on('exit', async () => {
                console.log(`worker died`);

                await QueueWorker.spawn();
            });
            worker.on('message', (message) => {
                if (message === 'ready') {
                    resolve();
                }
            });
        });
    }
}

