package com.alibabacloud.polar_race.engine.common.utils;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午2:58
 */

//key是8个字节，主要要实现comepare和hashcode
public class Key implements Comparable<Key>{
    private byte[] data;

    public Key(byte[] bytes){
        this.data = bytes;
    }

    public byte[] getBytes() {
        return data;
    }

    public int compareTo(Key that) {
        if (this == that) {
            return 0;
        }
        if (this.data == that.data) {
            return 0;
        }

        for (int i = 0; i < 8; i++) {
            int thisByte = 0xFF & this.data[i];
            int thatByte = 0xFF & that.data[i];
            if (thisByte != thatByte) {
                return (thisByte) - (thatByte);
            }
        }
        return 0;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (int i = 0; i < 8; i++) {
            hash = 31 * hash + data[i];
        }
        if (hash == 0) {
            hash = 1;
        }
        return hash;
    }
}
