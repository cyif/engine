package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.utils.ByteToInt;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;
import com.alibabacloud.polar_race.engine.common.utils.Key;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;


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
    private AtomicInteger wrotePosition = new AtomicInteger(0);//每次加1，代表第几个数据
    private ConcurrencyHashTable<Key, byte[]> map;


    public DBImpl(String path){

        this.map = new ConcurrencyHashTable<Key, byte[]>(1024*1024, 1024);

        //判断KeyLog文件是否存在,如果存在，进行内存恢复
        File dir = new File(path + File.separator + GlobalConfig.storePathKey);
        if (dir.exists()){
            recoverKeyLog(path);//keylog恢复和wroteposition恢复
            System.out.println("recoverKeylog finished");
            recoverHashtable();//hashtable恢复
            System.out.println("recoverHashtable finished");
        }

        //如果不存在，说明是第一次open
        else {
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path + File.separator + GlobalConfig.storePathKey);
        }

        //打开或创建valuelog文件
        this.valueLogs = new ValueLog[GlobalConfig.ValueFileNum];
        for (int i=0; i<GlobalConfig.ValueFileNum; i++){
            valueLogs[i] = new ValueLog(GlobalConfig.ValueFileSize, path + File.separator + GlobalConfig.storePathValue, i);
        }

    }

    private void recoverKeyLog(String path){
        keyLog = new KeyLog(GlobalConfig.KeyFileSize, path + File.separator + GlobalConfig.storePathKey);
        int length = keyLog.getFileLength();
        this.wrotePosition = new AtomicInteger(length / 12);

    }
    private void recoverHashtable(){
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);

        System.out.println(keyLog.getFileLength());
        while (byteBuffer.position()<keyLog.getFileLength()){
            byte[] key = new byte[8];
            byteBuffer.get(key, 0, 8);
            byte[] offset = new byte[4];
            byteBuffer.get(offset, 0, 4);

            map.put(new Key(key), offset);
        }
    }


    public void write(byte[] key, byte[] value){
        int currentPos = this.wrotePosition.getAndAdd(1);

        int key_wrotePosition = currentPos * 12;//keyLog的wrotePosition

        int value_file_no = (int)(((long) currentPos * 4096) / GlobalConfig.ValueFileSize);
        int value_file_wrotePosition = (int)(((long) currentPos * 4096) % GlobalConfig.ValueFileSize);

        //value写入value文件
        valueLogs[value_file_no].putMessage(value, value_file_wrotePosition);
        //写入hashmap的offset是除以4K的
        byte[] offset = ByteToInt.intToByteArray(currentPos);
        //key和offset写入key文件
        keyLog.putKey(key, offset, key_wrotePosition);
        //key和offset写入hashmap
        map.put(new Key(key), offset);
    }

    public byte[] read(byte[] key){
        byte[] offset = map.get(new Key(key));
        int currentPos = ByteToInt.byteArrayToInt(offset);
        long global_offset = (long) currentPos * 4096;
        int value_file_no = (int)(((long) currentPos * 4096) / GlobalConfig.ValueFileSize);
        int value_file_wrotePosition = (int)(((long) currentPos * 4096) % GlobalConfig.ValueFileSize);

        return valueLogs[value_file_no].getMessage(value_file_wrotePosition);
    }
}
