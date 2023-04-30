import { loremIpsum } from 'lorem-ipsum';
import QueueService from './queue-service.js';

const q = await QueueService.create();

const tasks = 10;

for (let i = 0; i < tasks; i += 1) {
    const text = loremIpsum();

    q.push({ text }, (err, result) => {
        if (err) {
            return console.error(`error: ${err.message}`);
        }

        console.log(`${result.words} words processed`);
    });

    console.log(`task #${i + 1} pushed`);
}

await q.run();

