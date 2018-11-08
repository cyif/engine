package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.utils.BloomFilter;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageLock;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageSpinLock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/11/8
 * Time: 上午10:36
 */
public class SortLog {
//    /*映射的fileChannel对象*/
//    private FileChannel fileChannel;
//    /*映射的内存对象*/
//    private MappedByteBuffer mappedByteBuffer;

    private int size;
    private long[] keyArray;
    private int[] offsetArray;
//    private BloomFilter<Long> bloomFilter;

    private static final int MAX_SIZE = 252000;


    public SortLog(){
        this.size = 0;
        this.keyArray = new long[MAX_SIZE];
        this.offsetArray = new int[MAX_SIZE];
//        double falsePositiveProbability = 0.2;
        int expectedSize = MAX_SIZE;
//        this.bloomFilter = new BloomFilter<>(falsePositiveProbability, expectedSize);
    }

//    public SortLog(int FileSize, String storePath, int fileName) {
//        /*打开文件，并将文件映射到内存*/
//        try {
//            ensureDirOK(storePath);
//            File file = new File(storePath, "sort" + fileName);
//            if (!file.exists()) {
//                try {
//                    file.createNewFile();
//                } catch (IOException e) {
//                    System.out.println("Create file" + "sort" + "failed");
//                    e.printStackTrace();
//                }
//            }
//            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
//            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);
//        } catch (FileNotFoundException e) {
//            System.out.println("create file channel " + "sort" + " Failed. ");
//        } catch (IOException e) {
//            System.out.println("map file " + "sort" + " Failed. ");
//        }
//
//        this.size = 0;
//        this.keyArray = new long[MAX_SIZE];
//        this.offsetArray = new int[MAX_SIZE];
//        double falsePositiveProbability = 0.1;
//        int expectedSize = MAX_SIZE;
//        this.bloomFilter = new BloomFilter<>(falsePositiveProbability, expectedSize);
//    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
            }
        }
    }


    //插入数据
    public void insert(long key, int offset) {
//        putMessageLock.lock();
//        putMessageLock.unlock();
//        if (bloomFilter.contains(key)) {
//            boolean flag = false;
//            for (int i = 0; i < size; i++)
//                if (keyArray[i] == key) {
//                    offsetArray[i] = offset;
//                    flag = true;
//                    break;
//                }
//            if (!flag) {
//                keyArray[size] = key;
//                offsetArray[size] = offset;
//                size++;
//                bloomFilter.add(key);
//            }
//
//        } else {
//            keyArray[size] = key;
//            offsetArray[size] = offset;
//            size++;
//            bloomFilter.add(key);
//        }
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
//                return middle;
            } else if (key < keyArray[middle]) {
                right = middle - 1;
            } else { // if numbers[middle] < find
                left = middle + 1;
            }
        }

        return -1;
    }

    //刷盘函数

    //读取函数

}
