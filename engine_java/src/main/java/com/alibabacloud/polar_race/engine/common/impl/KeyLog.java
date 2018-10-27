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

//    //获得文件写到哪里的长度,用于recover...不好使啊
//    public int getFileLength(){
//        int ans = 0;
//        try {
//            ans = (int) this.randomAccessFile.length();
//        }
//        catch (IOException e){
//            log.error("get file length Failed. ", e);
//        }
//        return ans;
//    }


    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                System.out.println(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }


    void putKey(byte[] key,int offset, int wrotePosition) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(wrotePosition);
        byteBuffer.put((byte) 1);
        byteBuffer.put(key, 0, 8);
        byteBuffer.putInt(offset);
    }


    //mappedbytebuffer读取数据,用于恢复hash
    ByteBuffer getKeyBuffer() {
        return this.mappedByteBuffer.slice();
    }

}
