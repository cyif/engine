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


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午2:45
 */

public class KeyLog {
    private static final Logger log = LoggerFactory.getLogger(KeyLog.class);

    /*文件的大小,1024*1024*64*12 就够。1个g足够*/
    private final int FileSize;

    private RandomAccessFile randomAccessFile;

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;

    public KeyLog(int FileSize, String storePath) {
        this.FileSize = FileSize;
        /*检查文件夹是否存在*/
        ensureDirOK(storePath);
        /*打开文件，并将文件映射到内存*/
        try {
            File file = new File(storePath + File.separator + 0);
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);
        } catch (FileNotFoundException e) {
            log.error("create file channel " + 0 + " Failed. ", e);
        } catch (IOException e) {
            log.error("map file " + 0 + " Failed. ", e);
        }
    }

    //获得文件写到哪里的长度,用于recover
    public int getFileLength(){
        int ans = 0;
        try {
            ans = (int) this.randomAccessFile.length();
        }
        catch (IOException e){
            log.error("get file length Failed. ", e);
        }
        return ans;
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
    void putKey(byte[] key, byte[] offset, int wrotePosition) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(wrotePosition);
        byteBuffer.putChar('1');
        byteBuffer.put(key, 0, 8);
        byteBuffer.put(offset, 0, 4);
    }

    //mappedbytebuffer读取数据,用于恢复hash
    ByteBuffer getKeyBuffer() {
        return this.mappedByteBuffer.slice();
    }

}
