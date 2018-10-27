package com.alibabacloud.polar_race.engine.common.impl;
import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;



import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    private int lastFileNo = 0;

    public DBImpl(String path){

        try {
            createDBPath(path);
        }catch (IOException e){
            System.out.println("create path error");
            e.printStackTrace();
        }

        //判断KeyLog文件是否存在,如果存在，进行内存恢复
        File dir = new File(path, "key");
        if (dir.exists()){
            System.out.println("---------------Start read or write append---------------");
            this.map = new ConcurrencyHashTable(512*1024, 0);
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path);//keylog恢复
            recoverHashtable();//hashtable恢复和wroteposition恢复
            System.out.println("Recover finished");
        }

        //如果不存在，说明是第一次open
        else {
            System.out.println("---------------Start first write---------------");
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path);
        }

        //打开或创建valuelog文件
        this.valueLogs = new ValueLog[GlobalConfig.ValueFileNum];
        for (int i=0; i<GlobalConfig.ValueFileNum; i++){
            valueLogs[i] = new ValueLog(GlobalConfig.ValueFileSize, path + File.separator, i);
        }

    }

    public static void createDBPath(String dbPath) throws IOException {
        Path path = Paths.get(dbPath);
        if (!Files.exists(path))
            Files.createDirectory(path);
    }

    private void recoverHashtable(){
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);

        while (true){
            if (byteBuffer.get() != (byte) 1)
                break;
            int now = this.wrotePosition.getAndAdd(1);//恢复wroteposition

            if (now % (1024*256) == 0)
                System.out.println("recover   " + now);

            byte[] key = new byte[8];
            byteBuffer.get(key, 0, 8);
            map.put(key, byteBuffer.getInt());
        }
    }


    public void write(byte[] key, byte[] value){
        int currentPos = this.wrotePosition.getAndAdd(1);
        int key_wrotePosition = currentPos * 13;//keyLog的wrotePosition


//        int value_file_no = (int)(((long) currentPos * 4096) / GlobalConfig.ValueFileSize);
        int value_file_wrotePosition = (int)(((long)currentPos * 4096) % GlobalConfig.ValueFileSize);
        int value_file_no = (int) currentPos / 262144;

        //clean
        if (value_file_no > lastFileNo){
            valueLogs[lastFileNo] = null;
        }
        else lastFileNo = value_file_no;

        //value写入value文件
        valueLogs[value_file_no].putMessage(value, value_file_wrotePosition);
        //key和offset写入key文件
        keyLog.putKey(key, currentPos, key_wrotePosition);
    }

    public byte[] read(byte[] key){
        int currentPos =  map.get(key);

//        int value_file_no = (int)((long) currentPos * 4096 / GlobalConfig.ValueFileSize);
        int value_file_wrotePosition = (int)(((long)currentPos * 4096) % GlobalConfig.ValueFileSize);
        int value_file_no = (int) currentPos / 262144;

        return valueLogs[value_file_no].getMessage(value_file_wrotePosition);
    }
}
