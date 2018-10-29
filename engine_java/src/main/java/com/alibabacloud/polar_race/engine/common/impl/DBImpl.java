package com.alibabacloud.polar_race.engine.common.impl;
import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageLock;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageReentrantLock;



import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;



/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午3:14
 */
public class DBImpl {
    private ValueLog valueLog;
    private KeyLog keyLog;
    private int wrotePosition;
    private ConcurrencyHashTable map;
    private PutMessageLock lock;


    public DBImpl(String path){

        try {
            createDBPath(path);
        }catch (IOException e){
            System.out.println("create path error");
            e.printStackTrace();
        }

        this.valueLog = new ValueLog(path);
        this.lock = new PutMessageReentrantLock();
        //判断KeyLog文件是否存在,如果存在，进行内存恢复
        File dir = new File(path, "key");
        if (dir.exists()){
            System.out.println("---------------Start read or write append---------------");
            this.map = new ConcurrencyHashTable(1024*1024, 0);
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


    }

    public static void createDBPath(String dbPath) throws IOException {
        Path path = Paths.get(dbPath);
        if (!Files.exists(path))
            Files.createDirectory(path);
    }

    private void recoverHashtable(){
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);

        this.wrotePosition = (int) (this.valueLog.getFileLength() / 4096);
        System.out.println(wrotePosition);

        int size = wrotePosition;

        while (size > 0){
            if (size % (1024*256) == 0)
                System.out.println("recover   " + size);

            byte[] key = new byte[8];
            byteBuffer.get(key, 0, 8);
            map.put(key, byteBuffer.getInt());

            size--;
        }
    }


    public void write(byte[] key, byte[] value){
        lock.lock();
        valueLog.putMessage(value, (long) wrotePosition*4096);
//        keyLog.putKey(key, wrotePosition, wrotePosition*12);
        int num = wrotePosition;
        wrotePosition ++;
        lock.unlock();

        keyLog.putKey(key, num, num*12);
    }

    public byte[] read(byte[] key) throws EngineException{
        int currentPos =  map.get(key);
        if (currentPos==-1)
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");

        long value_file_wrotePosition = (long)currentPos * 4096;

        return valueLog.getMessage(value_file_wrotePosition);
    }
}
