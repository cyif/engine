/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 上午10:32
 */


package com.alibabacloud.polar_race.engine.common.utils;

//hashtable 每个节点存的红黑树，写上锁。可选择自旋锁或者普通锁，以及锁的数量
public class ConcurrencyHashTable {
    private RBTree[] table;
    private PutHashLock[] locks;
    private int bucket_size;//桶的数量
//    private int lock_size;//锁的数量，锁平均分配到桶上
    private int ratio;

    public ConcurrencyHashTable(int bucket_size, int lock_size){
        this.bucket_size = bucket_size;
//        this.lock_size = lock_size;
        this.ratio = bucket_size / lock_size;
        this.table = new RBTree[bucket_size];
        this.locks = new PutHashLock[lock_size];

        for (int i=0; i<bucket_size; i++){
            table[i] = new RBTree();
        }
        for (int i=0; i<lock_size; i++){
            locks[i] = new PutHashSpinLock();
        }
    }

    public int get(byte[] key) {
        int hash = hash(key);
        RBTree node = table[hash];
        return node.get(key);
    }

    public void put(byte[] key, int offset) {
        int hash = hash(key);
        RBTree node = table[hash];
        //插入的时候需要上锁
        this.locks[hash / ratio].lock();
        node.insert(key, offset);
        locks[hash / ratio].unlock();
    }

    private int hash(byte[] key){
        int hash = 0;
        for (int i = 0; i < key.length; i++) {
            hash = 31 * hash + key[i];
        }
        if (hash == 0) {
            hash = 1;
        }

        return (hash & 0x7fffffff) % bucket_size;
    }


}

