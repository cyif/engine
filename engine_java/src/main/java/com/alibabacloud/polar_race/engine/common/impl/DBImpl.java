package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.utils.ByteToInt;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;
import com.alibabacloud.polar_race.engine.common.utils.Key;
import com.alibabacloud.polar_race.engine.common.utils.PutHashSpinLock;
import com.sun.tools.javac.comp.DeferredAttr;
import org.rocksdb.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午3:14
 */
public class DBImpl {
    private static final Logger log = LoggerFactory.getLogger(DBImpl.class);
    private ValueLog[] valueLogs;
    private KeyLog keyLog;
    private AtomicLong wrotePosition = new AtomicLong(0);
    private ConcurrencyHashTable<Key, byte[]> map;


    public DBImpl(String path){
        map = new ConcurrencyHashTable<Key, byte[]>(1024*1024, new PutHashSpinLock(), 1024);

        //判断KeyLog文件是否存在,如果存在，进行内存恢复
        File dir = new File(path + File.separator + GlobalConfig.storePathKey);
        if (dir.exists()){
            recoverKeyLog(path);
            recoverHashtable();
            recoverValueLog(path);
        }

        //如果不存在，说明是第一次open
        else {
            for (int i=0; i<GlobalConfig.ValueFileNum; i++){
                valueLogs[i] = new ValueLog(GlobalConfig.ValueFileSize, path + File.separator + GlobalConfig.storePathValue, false);
            }
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path + File.separator + GlobalConfig.storePathKey);
        }

    }

    public void recoverKeyLog(String path){
        keyLog = new KeyLog(GlobalConfig.KeyFileSize, path + File.separator + GlobalConfig.storePathKey);
        int length = keyLog.getFileLength();
        keyLog.setWrotePosition(length);

        this.wrotePosition = new AtomicLong(length * 4 * 1024 / 12);
    }
    public void recoverHashtable(){
        ByteBuffer byteBuffer = keyLog.getKey();
        byteBuffer.flip();

        while (byteBuffer.position()<keyLog.getFileLength()){
            byte[] key = new byte[8];
            byteBuffer.get(key, 0, 8);
            byte[] offset = new byte[4];
            byteBuffer.get(offset, 0, 4);

            map.put(new Key(key), offset);
        }
    }
    public void recoverValueLog(String path){
        for (int i=0; i<GlobalConfig.ValueFileNum; i++){
            valueLogs[i] = new ValueLog(GlobalConfig.ValueFileSize, path + File.separator + GlobalConfig.storePathValue + File.separator + i, true);
        }
    }


    public void write(byte[] key, byte[] value){
        long currentPos = this.wrotePosition.getAndAdd(4096);

        int file_no = (int) currentPos / GlobalConfig.ValueFileSize;
        int file_offset = (int) currentPos % GlobalConfig.ValueFileSize;
        int global_offset = (int) currentPos / 4096;
        //value写入value文件
        valueLogs[file_no].putMessage(value, file_offset);

        //写入hashmap的offset是除以4K的
        byte[] offset = ByteToInt.intToByteArray(global_offset);
        //key和offset写入key文件
        keyLog.putKey(key, offset);
        //key和offset写入hashmap
        map.put(new Key(key), offset);
    }

    public byte[] read(byte[] key){
        byte[] offset = map.get(new Key(key));
        int global_offset = ByteToInt.byteArrayToInt(offset);
        long global_offset1 = global_offset * 4096;
        int file_no = (int) global_offset1 / GlobalConfig.ValueFileSize;
        int file_offset = (int) global_offset1 % GlobalConfig.ValueFileSize;

        return valueLogs[file_no].getMessage(file_offset);
    }
}
