import _ from 'lodash';
import { Worker, parentPort } from 'node:worker_threads';
import { Router } from "zeromq"
import { randomInt } from 'node:crypto';

export default class QueueWorker {
    constructor() {
        this.socket = new Router({ receiveHighWaterMark: 1 });
    }

    async run() {
        await this.socket.connect('inproc://queue');

        parentPort.postMessage('ready');

        console.log(`worker is ready`);

        for await (const [sender, pos, msg] of this.socket) {
            const { uuid, text } = JSON.parse(msg);
            const words = _
                .chain(text)
                .words()
                .size()
                .value();
            const fakeProcessingTime = randomInt(200, 2000);

            setTimeout(async () => {
                console.log(`task: "${text}" (${words} words)`);

                const ok = words < 10;

                if (ok) {
                    await this.socket.send([
                        sender,
                        Buffer.from(''),
                        Buffer.from(JSON.stringify({ uuid, words }))
                    ]);
                }
                else {
                    this.stop();
                }
            }, fakeProcessingTime);
        }
    }

    async stop() {
        this.socket.close();
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

