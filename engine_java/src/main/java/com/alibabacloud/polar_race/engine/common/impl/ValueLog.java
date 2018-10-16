package com.alibabacloud.polar_race.engine.common.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午1:43
 */
public class ValueLog {
    private static final Logger log = LoggerFactory.getLogger("ValueLog");
    /*文件的大小,最好是1G*/
    private final int FileSize;

    /*映射的文件名*/
    private static AtomicInteger fileName = new AtomicInteger(0);//需要256个g的文件

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    public ValueLog(int FileSize, String storePath, boolean recover) {
        this.FileSize = FileSize;
        /*检查文件夹是否存在*/
        ensureDirOK(storePath);
        /*打开文件*/
        try {
            if (recover){
                File file = new File(storePath);
                this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            }
            else {
                File file = new File(storePath + File.separator + fileName.getAndIncrement());
                this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            }

        } catch (FileNotFoundException e) {
            log.error("create file channel " + fileName + " Failed. ", e);
        }
    }



    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    //directbytebuffer写一个数据
    void putMessage(byte[] value, int wrotePosition) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        byteBuffer.put(value);
        try {
            this.fileChannel.write(byteBuffer, wrotePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //directbytebuffer读取数据
    byte[] getMessage(int offset) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        try {
            this.fileChannel.read(byteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byteBuffer.flip();
        byte[] bytes = new byte[4096];
        byteBuffer.get(bytes);
        return bytes;
    }


}
