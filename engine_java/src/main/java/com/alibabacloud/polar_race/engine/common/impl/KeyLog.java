package com.alibabacloud.polar_race.engine.common.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午2:45
 */

public class KeyLog {
    /*文件的大小,1024*1024*64*12 就够。1个g足够*/
//    private final int FileSize;

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;


    public KeyLog(int FileSize, String storePath) {
//        this.FileSize = FileSize;
        /*检查文件夹是否存在*/
//        ensureDirOK(storePath);
        /*打开文件，并将文件映射到内存*/
        try {
            File file = new File(storePath, "key");
            if (!file.exists()){
                try {
                    file.createNewFile();
                }
                catch (IOException e){
                    System.out.println("Create file" + "key" + "failed");
                    e.printStackTrace();
                }
            }
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);

        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + "key" + " Failed. ");
        } catch (IOException e) {
            System.out.println("map file " + "key" + " Failed. ");
        }
    }

    void putKey(byte[] key,int offset, int wrotePosition) {
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(wrotePosition);
        byteBuffer.put(key);
        byteBuffer.putInt(offset);
    }

    void setWrotePosition(int wrotePosition){
        mappedByteBuffer.position(wrotePosition);
    }

    void putKey(byte[] key,int offset) {
        mappedByteBuffer.put(key);
        mappedByteBuffer.putInt(offset);
    }

    //mappedbytebuffer读取数据,用于恢复hash
    ByteBuffer getKeyBuffer() {
        return this.mappedByteBuffer.slice();
    }

}
