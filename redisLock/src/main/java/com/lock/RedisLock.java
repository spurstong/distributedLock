package com.lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Collections;
import java.util.UUID;

public class RedisLock {
    private final JedisPool jedisPool;

    private static ThreadLocal<Integer> states = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    };
    // 当key不存在时，我们进行set操作；当key存在时，则不作任何操作
    private static final String SET_IF_NOT_EXIST = "NX";
    // 为key添加一个过期操作
    private static final String SET_WITH_EXPIRE_TIME = "PX";
    private static final int DEFAULT_SLEEP_TIME = 10;
    private static final String LOCK_STATE = "OK";


    public RedisLock(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * 利用Redis实现加锁机制的第一个版本，但会出现一些错误，在博客中解释
     */
    public String addLockVersion1(String key, int blockTime, int expireTime) {
        if (blockTime <=0 || expireTime <= 0)
            return null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String sign = UUID.randomUUID().toString();
            String token = null;
            //设置阻塞尝试时间
            long endTime = System.currentTimeMillis() + blockTime;
            while (System.currentTimeMillis() < endTime) {
                if (jedis.setnx(key, sign) == 1) {
                    // 添加成功，设置锁的过期时间，防止死锁
                    jedis.expire(key, expireTime);
                    // 在释放锁时用于验证
                    token = sign;
                    return token;
                }
                //加锁失败，休眠一段时间，再进行尝试。
                try {
                    Thread.sleep(DEFAULT_SLEEP_TIME);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 利用redis实现加锁机制的第二个版本，但是也会出现一些问题，在博客中解释
     */
    public boolean addLockVersion2(String key, int blockTime, int expireTime) {
        if (blockTime <=0 || expireTime <= 0)
            return false;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            long endTime = System.currentTimeMillis() + blockTime;
            while (System.currentTimeMillis() < endTime) {
                long redisExpierTime = System.currentTimeMillis() + expireTime;
                if (jedis.setnx(key, redisExpierTime + "") == 1) {
                    jedis.expire(key, expireTime);
                    return true;
                } else {
                    String oldRedisExpierTime = jedis.get(key);
                    // 当锁设置成功，但是没有通过expire成功设置过期时间，但是根据存的值判断出它实际上已经过期了
                    if (oldRedisExpierTime != null && Long.parseLong(oldRedisExpierTime) < System.currentTimeMillis()) {
                        String lastRedisExpierTime = jedis.getSet(key, System.currentTimeMillis() + blockTime + "");
                        //获取到该锁，没有被其他线程所修改
                        if (lastRedisExpierTime.equals(oldRedisExpierTime)) {
                            jedis.expire(key, expireTime);
                            return true;
                        }
                    }
                }
                //加锁失败，休眠一段时间，再进行尝试。
                try {
                    Thread.sleep(DEFAULT_SLEEP_TIME);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     * 利用Redis实现的加锁机制第三版，可有效解决上面的问题
     * @param key 锁名
     * @param uniqueId 锁拥有标识
     * @param blockTime 阻塞时间
     * @param expireTime 过期时间
     * @return
     */
    public boolean addLockVersion3(String key, String uniqueId, int blockTime, int expireTime) {
        Jedis jedis = null;
        try {
            long endTime = System.currentTimeMillis() + blockTime;
            while (System.currentTimeMillis() < endTime) {
                jedis = jedisPool.getResource();
                String result = jedis.set(key, uniqueId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);
                if (LOCK_STATE.equals(result))
                    return true;
                try {
                    Thread.sleep(DEFAULT_SLEEP_TIME);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return false;
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return false;
    }

    /**
     * 基于Redis的加锁机制第四版，尝试改用可重入锁思想
     */
    public boolean addLockVersion4(String key, String uniqueId, int expireTime) {
        int state = states.get();
        if (state > 1) {
            states.set(state+1);
            return true;
        }
        return doLock(key, uniqueId, expireTime);
    }

    private boolean doLock(String key, String uniqueId, int expireTime) {
        Jedis jedis = null;
        if (expireTime <= 0)
            return false;
        try {
            jedis = jedisPool.getResource();
            String result = jedis.set(key, uniqueId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);
            if (LOCK_STATE.equals(result))
                states.set(states.get() + 1);
                return true;
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return false;
    }
    /**
     * 基于Redis的释放锁机制版本1，但是会产生一些问题，在博客中解释
     */
    public boolean releaseLockVersion1(String key, String uniqueId) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String lockId = jedis.get(key);
            if (lockId != null && lockId.equals(uniqueId)) {
                jedis.del(key);
                return true;
            }
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return false;
    }

    /**
     * 基于Redis的释放锁机制第二版
     */
    public boolean releaseLockVersion2(String key, String uniqueId) {
        String luaScript = "if  redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Jedis jedis = null;
        Object result = null;
        try{
            jedis = jedisPool.getResource();
            result = jedis.eval(luaScript, Collections.singletonList(key), Collections.singletonList(uniqueId));
            if ((Long)result == 1)
                return true;
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return false;
    }

    /**
     * 基于Redis的释放锁机制第三版，尝试利用可重入锁思想
     */
    public boolean releaseLockVersion3(String key, String uniqueId) {
        int state = states.get();
        if (state > 1) {
            states.set(states.get() - 1);
            return false;
        }
        return this.doRelease(key, uniqueId);

    }
    private boolean doRelease(String key, String uniqueId) {
        String luaScript = "if  redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Jedis jedis = null;
        Object result = null;
        try{
            jedis = jedisPool.getResource();
            result = jedis.eval(luaScript, Collections.singletonList(key), Collections.singletonList(uniqueId));
            if ((Long)result == 1)
                return true;
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            states.set(0);
            if (jedis != null)
                jedis.close();
        }
        return false;
    }
}
