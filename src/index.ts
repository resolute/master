import os from 'os';
import { shutdown } from '@resolute/runtime';
import memcache from '@resolute/memcache';
import { MemcacheClient, MemcacheOptions } from '@resolute/memcache/lib/types';

const masters = {};

export default async ({
    name = '',
    prefix = 'master-task-',
    uid = `${os.hostname}:${process.pid}`,
    duration = 60_000,
    cache,
}: {
    name: any,
    prefix?: string,
    uid?: string,
    duration?: number,
    cache: MemcacheClient | MemcacheOptions
}) => {

    if (typeof name !== 'string' || !name) {
        throw new Error('Preventing master process since `name` was not set in config.mjs.');
    }

    const key = prefix + name;

    if (!masters[key]) {
        masters[key] = new Promise((resolve) => {

            const { add, del, set } = (typeof cache === 'object' && 'add' in cache)
                ? cache // memcache client
                : memcache(cache); // memcache options

            const fraction = () => duration - .1 * duration +
                Math.random() * .2 * duration; // 70% - 90% of duration

            const stayMasterLoop = async () => {
                try {
                    await set(key, uid, duration / 1_000);
                } catch {
                    // squash any errors with memcache and stay as the master
                }
                setTimeout(stayMasterLoop, fraction());
            }

            const onMasterShutdown = async () => {
                try {
                    await del(key);
                } catch { }
            }

            const checkMasterLoop = async () => {
                try {
                    await add(key, uid, duration / 1_000);
                    stayMasterLoop();
                    shutdown(onMasterShutdown);
                    resolve();
                    return;
                } catch {
                    setTimeout(checkMasterLoop, fraction());
                }
            }

            setTimeout(checkMasterLoop, 2_000); // give time for dying SIGINT to del()
        });
    }

    return masters[key];
};