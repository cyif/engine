package com.alibabacloud.polar_race.engine.common.utils;



public class RBTree {
//    private static final boolean RED = true;
//    private static final boolean BLACK = false;

    private Node root;

    private class Node {
        long key;
        int offset;
        Node left = null;
        Node right = null;
        //红色是1，黑色是0,减小内存的使用
        byte color;

        Node(byte[] key, int offset, byte color) {
            this.key = ByteToInt.byteArrayToLong(key);
            this.offset = offset;
            this.color = color;
        }
    }

    private boolean isRed(Node x) {
        // 空节点属于黑节点
        if(x == null) return false;
        // 判断节点是否为红色
        return x.color == 1;
    }


    public int get(byte[] key) {
        Node x = root;
        while(x != null) {
            int compare = compareByte(key, x.key);
            if(compare < 0) {
                x = x.left;
            }
            else if(compare > 0) {
                x = x.right;
            }
            else {
                return x.offset;
            }
        }
        // 没有找到
        return -1;
    }



    public void insert(byte[] key, int offset) {
        root = put(root, key, offset);
    }

    private Node put(Node h, byte[] key, int offset) {
        // 创建一个新的红色的节点
        if(h == null) {
            return new Node(key, offset, (byte)1);
        }

        // 定位到需要插入的节点
        int compare = compareByte(key, h.key);

        if(compare < 0) {
            h.left = put(h.left, key, offset);
        } else if(compare > 0) {
            h.right = put(h.right, key, offset);
        } else {
            h.offset = offset;
        }

        // 调整红黑树，使其平衡
        if(isRed(h.right) && !isRed(h.left)) {
            h = rotateLeft(h);
        }
        if(isRed(h.left) && isRed(h.left.left)) {
            h = rotateRight(h);
        }
        if(isRed(h.left) && isRed(h.right)) {
            flipColor(h);
        }

        return h;
    }




    private Node rotateLeft(Node h) {
        Node x = h.right;
        h.right = x.left;
        x.left = h;
        x.color = h.color;
        h.color = 1;
        return x;
    }

    private Node rotateRight(Node h) {
        Node x = h.left;
        h.left = x.right;
        x.right = h;
        x.color = h.color;
        h.color = 1;
        return x;
    }

    private void flipColor(Node h) {
        h.color = 1;
        h.left.color = 0;
        h.right.color = 0;
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

}
