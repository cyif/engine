package com.alibabacloud.polar_race.engine.common.utils;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/26
 * Time: 上午10:43
 */
public class ListDB {
    private Node root;

    private class Node {
        long key;
        int offset;
        Node next = null;


        Node(byte[] key, int offset) {
            this.key = ByteToInt.byteArrayToLong(key);
            this.offset = offset;
        }
    }

    private int compareByte(byte[] bytes1, long key){
        byte[] bytes2 = ByteToInt.longToByteArray(key);
        for (int i=0; i<bytes1.length; i++){
            int thisByte = 0xFF & bytes1[i];
            int thatByte = 0xFF & bytes2[i];
            if (thisByte != thatByte)
                return thisByte - thatByte;
        }

        return bytes1.length - bytes2.length;
    }

    //插入到最前方
    public void insert(byte[] key, int offset) {
        Node node = new Node(key, offset);
        node.next = root;
        root = node;
    }

    public int get(byte[] key) {
        Node x = root;
        while(x != null) {
            int compare = compareByte(key, x.key);
            if(compare != 0) {
                x = x.next;
            }
            else {
                return x.offset;
            }
        }
        // 没有找到
        return -1;
    }
}
