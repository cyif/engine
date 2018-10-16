package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.utils.ByteToInt;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;
import com.alibabacloud.polar_race.engine.common.utils.Key;
import com.alibabacloud.polar_race.engine.common.utils.PutHashSpinLock;
import org.rocksdb.Slice;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午3:14
 */
public class DBImpl {
    private ValueLog[] valueLogs;
    private KeyLog keyLog;
    private AtomicLong wrotePosition = new AtomicLong(0);
    private ConcurrencyHashTable<Key, byte[]> map;


    public DBImpl(){
        map = new ConcurrencyHashTable<Key, byte[]>(1024*1024, new PutHashSpinLock(), 1024);

        for (int i=0; i<GlobalConfig.ValueFileNum; i++){
            valueLogs[i] = new ValueLog(GlobalConfig.ValueFileSize, GlobalConfig.storePathValue);
        }
        keyLog = new KeyLog(GlobalConfig.ValueFileSize, GlobalConfig.storePathKey);
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
