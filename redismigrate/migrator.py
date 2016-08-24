# -*- coding: utf-8 -*-

import json
import multiprocessing
import time

import redis

current_time = lambda: int(round(time.time() * 1000))


class Migrator(object):
    def __init__(self, config_file):
        with open(config_file) as data_file:
            configs = json.load(data_file)

        self.origin_redis_addrs = []
        self.target_redis_addrs = []

        if 'hosts' in configs['origin_cluster']:
            for i in configs['origin_cluster']['hosts']:
                s = i.split(':')
                self.origin_redis_addrs.append({'host': s[0], 'port': s[1]})
        else:
            self.origin_redis_addrs = [
                {'host': configs['origin_cluster']['host_pattern'].format(i),
                 'port': configs['origin_cluster']['port_pattern'].format(i)}
                for i in range(configs['origin_cluster']['start'], configs['origin_cluster']['end'] + 1)]

        if 'hosts' in configs['target_cluster']:
            for i in configs['target_cluster']['hosts']:
                s = i.split(':')
                self.target_redis_addrs.append({'host': s[0], 'port': s[1]})
        else:
            self.target_redis_addrs = [
                {'host': configs['target_cluster']['host_pattern'].format(i),
                 'port': configs['target_cluster']['port_pattern'].format(i)}
                for i in range(configs['target_cluster']['start'], configs['target_cluster']['end'] + 1)]

        self.key = configs['key']

    def migrate(self):
        results = []
        pool = multiprocessing.Pool(len(self.origin_redis_addrs))

        start_time = current_time()

        for r in self.origin_redis_addrs:
            results.append(
                pool.apply_async(scan_origin_server, args=(r, self.target_redis_addrs, self.key, '_handle_{}')))

        pool.close()
        pool.join()

        end_time = current_time()

        total_success = 0
        total_fail = 0
        for result in results:
            count = result.get()
            total_success += count[0]
            total_fail += count[1]

        print 'Migration mission complete'
        print 'Total success: {}. Total fail: {}. Total cost: {}'.format(total_success, total_fail,
                                                                         (end_time - start_time))

    def verify(self):
        results = []
        pool = multiprocessing.Pool(len(self.origin_redis_addrs))

        start_time = current_time()

        for r in self.origin_redis_addrs:
            results.append(
                pool.apply_async(scan_origin_server, args=(r, self.target_redis_addrs, self.key, '_verify_{}')))

        pool.close()
        pool.join()

        end_time = current_time()

        total_success = 0
        total_fail = 0
        for result in results:
            count = result.get()
            total_success += count[0]
            total_fail += count[1]

        print 'Verify mission complete'
        print 'Total success: {}. Total fail: {}. Total cost: {}'.format(total_success, total_fail,
                                                                         (end_time - start_time))


def scan_origin_server(origin_server_addr, target_server_addrs, key_config, methodPattern):
    try:
        is_success = True
        success_count = 0
        fail_count = 0

        method = globals()[methodPattern.format(key_config['type'])]

        origin_server = redis.Redis(host=origin_server_addr['host'], port=origin_server_addr['port'])
        target_servers = [redis.Redis(host=ts['host'], port=ts['port']) for ts in target_server_addrs]

        # scan keys
        for key in origin_server.scan_iter(match=key_config['pattern'], count=100):
            print 'key: {}'.format(key)
            try:
                router_method = eval(key_config['router'])
                is_success = method(origin_redis=origin_server, target_redis=target_servers[router_method(key)],
                                    key=key)

            except Exception as e:
                is_success = False
                print "exception: {}".format(e)
                continue

            if is_success:
                success_count += 1
            else:
                fail_count += 1

            time.sleep(0.001)

        return success_count, fail_count
        print ('Origin server: {}. Total success: {}. Total fail: {}'.format(origin_server, success_count, fail_count))

    except Exception as e:
        print ('Exception in scan_origin_server: {}'.format(e))


def _handle_string(origin_redis, target_redis, key):
    is_success = True

    try:
        value = origin_redis.get(key)
        target_redis.set(key, value)
        # print ('key: {}. value: {}'.format(key, value))
    except Exception as e:
        print ('handle string error. key: {}. error: {}'.format(key, e))
        is_success = False

    return is_success


def _handle_zset(origin_redis, target_redis, key):
    is_success = True

    try:
        values = []
        i = 0

        for value in origin_redis.zscan_iter(name=key, count=100):
            # print 'zset get key: {}. value:{}'.format(key, value)
            for v in value:
                values.append(v)
            i += 1
            if i >= 100:
                #print 'zset insert key: {}. values:{}'.format(key, values)
                target_redis.zadd(key, *values)
                i = 0
                values = []
                time.sleep(0.001)

        if len(values) > 0:
            # print 'zset insert key: {}. values:{}'.format(key, values)
            target_redis.zadd(key, *values)
            time.sleep(0.001)

    except Exception as e:
        print ('handle zset error. key: {}. error: {}'.format(key, e))
        is_success = False

    return is_success


def _handle_hash(origin_redis, target_redis, key):
    is_success = True

    try:
        values = {}
        i = 0

        for value in origin_redis.hscan_iter(name=key, count=100):
            # print 'hash get key: {}. value: {}.'.format(key, value)
            values[value[0]] = value[1]

            i += 1
            if i >= 100:
                # print 'hash set key: {}. values: {}'.format(key, values)
                target_redis.hmset(key, values)
                i = 0
                values = {}
                time.sleep(0.001)

        if len(values) > 0:
            # print 'hash set key: {}. values: {}'.format(key, values)
            target_redis.hmset(key, values)
            time.sleep(0.001)

    except Exception as e:
        print ('handle hash error. key: {}. error: {}'.format(key, e))
        is_success = False

    return is_success


def _verify_string(origin_redis, target_redis, key):
    is_equal = True

    try:
        origin_value = origin_redis.get(key)
        target_value = target_redis.get(key)

        if origin_value != target_value:
            is_equal = False
            print ('find unequal key: {}. origin_value:{}. target_value:{}'.format(key, origin_value, target_value))
            # 修正数据
            if origin_value is not None:
                print ('correct unequal key: {}'.format(key))
                target_redis.delete(key)
                _handle_string(origin_redis, target_redis, key)

    except Exception as e:
        print('find unequal key: {}. error: {}'.format(key, e))

    return is_equal


def _verify_zset(origin_redis, target_redis, key):
    is_equal = True

    try:
        origin_count = origin_redis.zcount(key, '-inf', '+inf')
        target_count = target_redis.zcount(key, '-inf', '+inf')

        if origin_count != target_count:
            is_equal = False
            print ('find unequal key: {}'.format(key))
            #修正数据
            if origin_count != 0:
                print ('correct unequal key: {}'.format(key))
                target_redis.delete(key)
                _handle_zset(origin_redis, target_redis, key)

    except Exception as e:
        print('find unequal key: {}. error: {}'.format(key, e))

    return is_equal


def _verify_hash(origin_redis, target_redis, key):
    is_equal = True

    try:
        origin_count = origin_redis.hlen(key)
        target_count = target_redis.hlen(key)

        if origin_count != target_count:
            is_equal = False
            print ('find unequal key: {}'.format(key))
            #修正数据
            if origin_count != 0:
                print ('correct unequal key: {}'.format(key))
                target_redis.delete(key)
                _handle_hash(origin_redis, target_redis, key)

    except Exception as e:
        print('find unequal key: {}. error: {}'.format(key, e))

    return is_equal