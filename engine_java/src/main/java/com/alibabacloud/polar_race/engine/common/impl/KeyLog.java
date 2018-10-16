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
 * Time: 下午2:45
 */
public class KeyLog {
    private static final Logger log = LoggerFactory.getLogger("KeyLog");
    /*文件的大小,1024*1024*64*12 就够。1个g足够*/
    private final int FileSize;

    /*映射的文件名*/
    private static AtomicInteger fileName = new AtomicInteger(0);

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;

    /*文件尾指针*/
    private AtomicInteger wrotePosition = new AtomicInteger(0);

    public KeyLog(int FileSize, String storePath) {
        this.FileSize = FileSize;
        /*检查文件夹是否存在*/
        ensureDirOK(storePath);
        /*打开文件，并将文件映射到内存*/
        try {
            File file = new File(storePath + File.separator + fileName.getAndIncrement());
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, FileSize);

        } catch (FileNotFoundException e) {
            log.error("create file channel " + fileName + " Failed. ", e);
        } catch (IOException e) {
            log.error("map file " + fileName + " Failed. ", e);
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
    int putKey(byte[] key, byte[] offset) {
        int currentPos = this.wrotePosition.getAndAdd(12);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(12);
        byteBuffer.put(key);
        byteBuffer.put(offset);
        try {
            this.fileChannel.write(byteBuffer, currentPos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return currentPos;
    }

    //mappedbytebuffer读取数据,用于恢复hash
    ByteBuffer getKey(int offset) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(offset);
        byteBuffer.limit(offset+12);
        return byteBuffer;
    }


}
