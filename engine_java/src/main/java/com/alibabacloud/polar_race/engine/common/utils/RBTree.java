package com.alibabacloud.polar_race.engine.common.utils;

public class RBTree <Key extends Comparable<Key>, Value>  {
//    private static final boolean RED = true;
//    private static final boolean BLACK = false;
    private Node root;

    private class Node {
        Key key;
        Value value;
        Node left = null;
        Node right = null;

        boolean color;

        Node(Key key, Value value, boolean color) {
            this.key = key;
            this.value = value;
            this.color = color;
        }
    }

    private boolean isRed(Node x) {
        // 空节点属于黑节点
        if(x == null) return false;
        // 判断节点是否为红色
        return x.color == true;
    }

    public Value get(Key key) {
        Node x = root;
        while(x != null) {
            int compare = key.compareTo(x.key);
            if(compare < 0) {
                x = x.left;
            }
            else if(compare > 0) {
                x = x.right;
            }
            else {
                return x.value;
            }
        }
        // 没有找到
        return null;
    }



    public void insert(Key key, Value value) {
        root = put(root, key, value);
    }

    private Node put(Node h, Key key, Value value) {
        // 创建一个新的红色的节点
        if(h == null) {
            return new Node(key, value, true);
        }

        // 定位到需要插入的节点
        int compare = key.compareTo(h.key);
        if(compare < 0) {
            h.left = put(h.left, key, value);
        } else if(compare > 0) {
            h.right = put(h.right, key, value);
        } else {
            h.value = value;
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
        h.color = true;
        return x;
    }

    private Node rotateRight(Node h) {
        Node x = h.left;
        h.left = x.right;
        x.right = h;
        x.color = h.color;
        h.color = true;
        return x;
    }

    private void flipColor(Node h) {
        h.color = true;
        h.left.color = false;
        h.right.color = false;
    }


}
