/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 上午10:32
 */


package com.alibabacloud.polar_race.engine.common.utils;

//hashtable 每个节点存的红黑树，写上锁。可选择自旋锁或者普通锁，以及锁的数量
public class ConcurrencyHashTable <Key extends Comparable<Key>, Value> {
    private RBTree<Key, Value>[] table;
    private PutHashLock[] locks;
    private int bucket_size;//桶的数量
    private int lock_size;//锁的数量，锁平均分配到桶上

    public ConcurrencyHashTable(int bucket_size, PutHashLock putMessageLock, int lock_size){
        this.bucket_size = bucket_size;
        this.lock_size = lock_size;
        this.table = new RBTree[bucket_size];
        this.locks = new PutHashLock[lock_size];
    }

    public Value get(Key key) {
        int hash = hash(key);
        RBTree<Key, Value> node = table[hash];
        return node.get(key);
    }

    public void put(Key key, Value value) {
        int hash = hash(key);
        RBTree<Key, Value> node = table[hash];
        //插入的时候需要上锁
        locks[hash%lock_size].lock();
        node.insert(key, value);
        locks[hash%lock_size].unlock();
    }

    private int hash(Key key){
        return (key.hashCode() & 0x7fffffff) % bucket_size;
    }

}

