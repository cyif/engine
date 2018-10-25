package com.alibabacloud.polar_race.engine.common.impl;
import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;



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
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path + File.separator + GlobalConfig.storePathKey);//keylog恢复
            recoverHashtable();//hashtable恢复和wroteposition恢复
            System.out.println("Recover finished");
        }

        //如果不存在，说明是第一次open
        else {
            System.out.println("---------------Start first write---------------");
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path + File.separator + GlobalConfig.storePathKey);
        }

        //打开或创建valuelog文件
        this.valueLogs = new ValueLog[GlobalConfig.ValueFileNum];
        for (int i=0; i<GlobalConfig.ValueFileNum; i++){
            valueLogs[i] = new ValueLog(GlobalConfig.ValueFileSize, path + File.separator + GlobalConfig.storePathValue, i);
        }

    }


    private void recoverHashtable(){
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);

        while (true){
            if (byteBuffer.get() != (byte) 1)
                break;
            this.wrotePosition.getAndAdd(1);//恢复wroteposition
            byte[] key = new byte[8];
            byteBuffer.get(key, 0, 8);
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
        //key和offset写入key文件
        keyLog.putKey(key, currentPos, key_wrotePosition);
    }

    public byte[] read(byte[] key){
        int currentPos =  map.get(key);
        long global_offset = (long) currentPos * 4096;
        int value_file_no = (int)(global_offset / GlobalConfig.ValueFileSize);
        int value_file_wrotePosition = (int)(global_offset % GlobalConfig.ValueFileSize);

        return valueLogs[value_file_no].getMessage(value_file_wrotePosition);
    }
}
