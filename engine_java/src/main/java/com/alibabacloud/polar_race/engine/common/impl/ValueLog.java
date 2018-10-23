package com.alibabacloud.polar_race.engine.common.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午1:43
 */
public class ValueLog {
    private static final Logger log = LoggerFactory.getLogger(ValueLog.class);

    /*文件的大小,最好是1G*/
//    private final int FileSize;

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;

    public ValueLog(int FileSize, String storePath, int fileName) {
//        this.FileSize = FileSize;
        /*检查文件夹是否存在*/
        ensureDirOK(storePath);
        /*打开文件*/
        try {
            File file = new File(storePath + File.separator + fileName);
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);

        } catch (FileNotFoundException e) {
            log.error("create file channel " + fileName + " Failed. ", e);
        } catch (IOException e) {
            log.error("map file " + 0 + " Failed. ", e);
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

    //mappedbytebuffer写一个数据
    void putMessage(byte[] value, int wrotePosition) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(wrotePosition);
        byteBuffer.put(value, 0, 4096);
    }

    //mappedbytebuffer读取数据
    byte[] getMessage(int offset) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(offset);
        byte[] bytes = new byte[4096];
        byteBuffer.get(bytes);
        return bytes;
    }


}
