import _ from 'lodash';
import Async from 'async';
import { Dealer } from 'zeromq';
import QueueWorker from './queue-worker.js';
import { randomUUID } from 'node:crypto';

export default class QueueService {
    constructor(concurrency) {
        this.callbacks = new Map();
        this.concurrency = concurrency;
        this.q = Async.queue(
            _.bind(this.queueWorkerFn, this),
            concurrency);
        this.socket = new Dealer();
        this.timeouts = new Map();
    }

    queueWorkerFn(task, cb) {
        const uuid = randomUUID();
        const envelope = [
            Buffer.from(''),
            Buffer.from(JSON.stringify({ uuid, ...task }))
        ];

        this.callbacks.set(uuid, cb);
        this.timeouts.set(uuid, setTimeout(() => {
            const cb = this.callbacks.get(uuid);

            this.callbacks.delete(uuid);
            this.timeouts.delete(uuid);

            _.attempt(cb, new Error('it took too long'));
        }, 3000));

        this.socket.send(envelope)
            .catch((err) => {
                this.cleanup(uuid);

                cb(err);
            });
    }

    push(...args) {
        return this.q.push(...args);
    }

    cleanup(uuid) {
        const to = this.timeouts.get(uuid);

        this.callbacks.delete(uuid);
        this.timeouts.delete(uuid);

        clearTimeout(to);
    }

    static async create(concurrency = 2) {
        const q = new QueueService(concurrency);

        await q.socket.bind('inproc://queue');

        await Promise.all(
            _.times(concurrency, QueueWorker.spawn));

        return q;
    }

    async run() {
        for await (const [pos, msg] of this.socket) {
            const { uuid, ...result } = JSON.parse(msg);
            const cb = this.callbacks.get(uuid);

            this.cleanup(uuid);

            _.attempt(cb, null, result);
        }
    }
}

