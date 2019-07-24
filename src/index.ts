import os from 'os';
import { shutdown } from '@resolute/runtime';

const masters = {};

interface MemcacheLike {
    add: Function; // (key: string, value: any, ttl?: number) => Promise<any>;
    del: Function; // (key: string) => Promise<any>;
    get: Function; // (key: string) => Promise<any>;
    set: Function; // (key: string, value: any, ttl?: number) => Promise<any>;
}

export default async ({
    name = '',
    prefix = 'master-task-',
    uid = `${os.hostname}:${process.pid}`,
    duration = 60000,
    cache,
}: {
    name: any,
    prefix?: string,
    uid?: string,
    duration?: number,
    cache: MemcacheLike
}) => {

    if (typeof name !== 'string' || !name) {
        throw new Error('Preventing master process since `name` was not set in config.mjs.');
    }

    const key = prefix + name;

    if (!masters[key]) {
        masters[key] = new Promise(resolve => {
            const { add, del, get, set } = cache;

            const fraction = () => duration - .1 * duration +
                Math.random() * .2 * duration; // 70% - 90% of duration

            const stayMasterLoop = () => set(key, uid, duration / 1000)
                .catch(() => { }) // squash any errors with memcache and stay as the master
                .then(() => { setTimeout(stayMasterLoop, fraction()); });

            const checkMasterLoop = () =>
                add(key, uid, duration / 1000)
                    .then(() => uid)
                    .catch(() => get(key))
                    .then(val => {
                        if (val === uid || val === null) {
                            throw new Error('Someone else is still master');
                        }
                    })
                    .then(stayMasterLoop)
                    .then(resolve)
                    .then(() => shutdown(() => del(key)))
                    .catch(() => { setTimeout(checkMasterLoop, fraction()); });

            setTimeout(checkMasterLoop, 2000); // give time for dying SIGINT to del()
        });
    }

    return masters[key];
};