# redis-migrator

redis data migrating tools

**support:**

* from cluster to cluster
* from single node to cluster
* from cluster to single node
* from single node to single node

# Usage

## dependency

* python 2.7
* redis-py

## Config files

Use json format config files. 
Copy and change the configs under ```configs/``` folder


e.g. From cluster to cluster:

```
{
  "origin_cluster": {
    "host_pattern": "redis_origin_cluster_{}_slave.com",
    "port_pattern": "637{}",
    "start": 0,
    "end": 9
  },
  "target_cluster": {
    "host_pattern": "redis_target_cluster_{}_master.com",
    "port_pattern": "637{}",
    "start": 0,
    "end": 9
  },
  "key": {
    "pattern": "key:*:status",
    "type": "string",
    "router": "lambda key : int(key.split(':')[1]) % 10"
  }
}
```

e.g. From single node to cluster:

```
{
  "origin_cluster": {
    "hosts": ["redis_node1_slave.com:6379", "redis_node2_slave.com:6379"]
  },
  "target_cluster": {
    "host_pattern": "redis_target_cluster_{}.com",
    "port_pattern": "637{}",
    "start": 0,
    "end": 9
  },
  "key": {
    "pattern": "key:*:version",
    "type": "string",
    "router": "lambda key : int(key.split(':')[1]) % 10"
  }
}
```


**Configs key point:***

* key.pattern - key format for scan match
* key.type - current support string/zset/hash
* key.router - for cluster, the rules to router to target clusters

## Command

**for migration**

```
python redis_migrate configs/config.json migrate 
```

**for verification**

```
python redis_migrate configs/config.json verify
```