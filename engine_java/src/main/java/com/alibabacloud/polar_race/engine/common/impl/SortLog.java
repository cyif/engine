package com.alibabacloud.polar_race.engine.common.impl;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/11/8
 * Time: 上午10:36
 */
public class SortLog {

    private int size;
    private long[] keyArray;
    private int[] offsetArray;

    public SortLog(int sortSize) {
        this.size = 0;
        this.keyArray = new long[sortSize];
        this.offsetArray = new int[sortSize];
    }


    //插入数据
    public void insert(long key, int offset) {
        keyArray[size] = key;
        offsetArray[size] = offset;
        size++;
    }

    public void quicksort() {
        quicksort(0, size - 1);
    }

    //快排
    private void quicksort(int pLeft, int pRight) {

        if (pLeft < pRight) {
            int storeIndex = partition(pLeft, pRight);
            quicksort(pLeft, storeIndex - 1);
            quicksort(storeIndex + 1, pRight);
        }
    }

    private int partition(int pLeft, int pRight) {

        swap(pRight, (pLeft + pRight) / 2);


        long pivotValue = keyArray[pRight];
        int storeIndex = pLeft;
        for (int i = pLeft; i < pRight; i++) {
            if (keyArray[i] < pivotValue) {
                swap(i, storeIndex);
                storeIndex++;
            }
        }
        swap(storeIndex, pRight);
        return storeIndex;
    }

    private void swap(int left, int right) {
//        keyArray[left] ^= keyArray[right] ^= keyArray[left] ^= keyArray[right];
//        offsetArray[left] ^= offsetArray[right] ^= offsetArray[left] ^= offsetArray[right];

        long tmp_key = keyArray[left];
        keyArray[left] = keyArray[right];
        keyArray[right] = tmp_key;

        int tmp_offset = offsetArray[left];
        offsetArray[left] = offsetArray[right];
        offsetArray[right] = tmp_offset;
    }

    //二分查找,查找key所在位置，找不到返回-1


    public int find(long key) {
        int left = 0;
        int right = size - 1;
        int middle;
        while (left <= right) {
            middle = left + (right - left) / 2;
            if (keyArray[middle] == key) {
                return offsetArray[middle];
            } else if (key < keyArray[middle]) {
                right = middle - 1;
            } else {
                left = middle + 1;
            }
        }

        return -1;
    }

//    //index相当于0-3
//    public int find(long key, int index) {
//
//        int left = (int)((size-1)/256 * index * 0.5);
//        if (left < 0)
//            left = 0;
//
//        int right = (int)((size - 1) / 256 * (index+1) * 2);
//        if (right > size-1)
//            right = size - 1;
//
//        int middle;
//
//        while (left <= right) {
//            middle = left + (right - left) / 2;
//            if (keyArray[middle] == key) {
//                return offsetArray[middle];
////                return middle;
//            } else if (key < keyArray[middle]) {
//                right = middle - 1;
//            } else { // if numbers[middle] < find
//                left = middle + 1;
//            }
//        }
//
//        left = 0;
//        right = size - 1;
//
//
//
//        //针对阶段一
//        while (left <= right) {
//            middle = left + (right - left) / 2;
//            if (keyArray[middle] == key) {
//                return offsetArray[middle];
////                return middle;
//            } else if (key < keyArray[middle]) {
//                right = middle - 1;
//            } else { // if numbers[middle] < find
//                left = middle + 1;
//            }
//        }
//
//        return -1;
//    }

}
