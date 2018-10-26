package com.alibabacloud.polar_race.engine.common.utils;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/26
 * Time: 下午9:40
 */
public class LinearProbeHashMap {
    private byte[] key_map;
    private int[] offset_map;
    private int bucket_size;
    //bucket_size最小 64*1024*1024
    public LinearProbeHashMap(int bucket_size){
        key_map = new byte[bucket_size * 9];
        offset_map = new int[bucket_size];
        this.bucket_size = bucket_size;
        System.out.println("new map successfully");
    }

    //为什么是9。第一位1或0代表是否这个位置已经存在
    public void put(byte[] key, int offset){
        int hash = hash(key);

        for(int i=0; i < bucket_size; i++) {
            int offset_index = (hash + i) % bucket_size;
            int key_index = offset_index * 9;
            // 找到了空位
            if (key_map[key_index] == 0) {
                key_map[key_index] = 1;
                for (byte b : key)
                    key_map[++key_index] = b;

                offset_map[offset_index] = offset;
                return;
            }

            // 找到了已经存在的值,替换
            if (equalKey(key_index + 1, key)) {
                offset_map[offset_index] = offset;
                return;
            }
        }
    }

    public int get(byte[] key){
        int hash = hash(key);
        for(int i=0; i < bucket_size; i++) {
            int offset_index = (hash + i) % bucket_size;
            int key_index = offset_index * 9;

            // 找到了该键
            if(equalKey(key_index + 1, key)) {
                return offset_map[offset_index];
            }
        }
        return -1;
    }

    private boolean equalKey(int start, byte[] key){
        for (int i=0; i<key.length; i++){
            if (key_map[start+i] != key[i])
                return false;
        }
        return true;
    }

    private int hash(byte[] key){
        int hash = 0;
        for (int i : key) {
            hash = 131 * hash + i;
        }
        if (hash == 0) {
            hash = 1;
        }

        return (hash & 0x7fffffff) % bucket_size;

//        int h = GlobalConfig.kFinish;
//        for (int i = 0; i < key.length; i++) {
//            h = (h * GlobalConfig.kA) ^ (key[0] * GlobalConfig.kB);
//        }
//        return (h & 0x7fffffff) % bucket_size;
    }
}
