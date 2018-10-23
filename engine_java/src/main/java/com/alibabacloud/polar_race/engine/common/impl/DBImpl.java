package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.utils.ByteToInt;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;
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
    private ConcurrencyHashTable map;


    public DBImpl(String path){

        //判断KeyLog文件是否存在,如果存在，进行内存恢复
        File dir = new File(path + File.separator + GlobalConfig.storePathKey);
        if (dir.exists()){
            System.out.println("---------------Start read---------------");
            this.map = new ConcurrencyHashTable(64*1024*1024, 1024);
            recoverKeyLog(path);//keylog恢复
            System.out.println("recoverKeylog finished");
            recoverHashtable();//hashtable恢复和wroteposition恢复
            System.out.println("recoverHashtable finished");
        }

        //如果不存在，说明是第一次open
        else {
            System.out.println("---------------Start write---------------");
            System.out.println("KeyFileSize " + GlobalConfig.KeyFileSize);
            System.out.println("ValueFileNum " + GlobalConfig.ValueFileNum);
            System.out.println("ValueFileSize " + GlobalConfig.ValueFileSize);

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
//        int length = keyLog.getFileLength();
//        this.wrotePosition = new AtomicInteger(length / 12);

    }
    private void recoverHashtable(){
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);

        while (true){
            if (byteBuffer.get() != (byte) 1)
                break;

            this.wrotePosition.getAndAdd(1);
            byte[] key = new byte[8];
            byteBuffer.get(key, 0, 8);
//            byte[] offset = new byte[4];
//            byteBuffer.get(offset, 0, 4);

            map.put(key, byteBuffer.getInt());
        }
    }


    public void write(byte[] key, byte[] value){
        int currentPos = this.wrotePosition.getAndAdd(1);

        int key_wrotePosition = currentPos * 13;//keyLog的wrotePosition

        int value_file_no = (int)(((long) currentPos * 4096) / GlobalConfig.ValueFileSize);
        int value_file_wrotePosition = (int)(((long) currentPos * 4096) % GlobalConfig.ValueFileSize);
        //value写入value文件
        valueLogs[value_file_no].putMessage(value, value_file_wrotePosition);
        //写入hashmap的offset是除以4K的
//        byte[] offset = ByteToInt.intToByteArray(currentPos);
        //key和offset写入key文件
        keyLog.putKey(key, currentPos, key_wrotePosition);
        //key和offset写入hashmap
//        map.put(new Key(key), offset);
//        map.put(new InternalKey(key, currentPos));
    }

    public byte[] read(byte[] key){
//        byte[] offset = map.get(new Key(key));
//        int currentPos = ByteToInt.byteArrayToInt(offset);

        int currentPos =  map.get(key);
        long global_offset = (long) currentPos * 4096;
        int value_file_no = (int)(global_offset / GlobalConfig.ValueFileSize);
        int value_file_wrotePosition = (int)(global_offset % GlobalConfig.ValueFileSize);

        return valueLogs[value_file_no].getMessage(value_file_wrotePosition);
    }
}
