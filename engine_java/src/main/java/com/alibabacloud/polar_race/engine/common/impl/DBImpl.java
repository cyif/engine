package com.alibabacloud.polar_race.engine.common.impl;
import com.alibabacloud.polar_race.engine.common.config.GlobalConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.ByteToInt;
import gnu.trove.map.hash.TLongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午3:14
 */
public class DBImpl {
    private ValueLog valueLog;
    private KeyLog keyLog;
    private TLongIntHashMap tmap;

//    private AtomicInteger readNum = new AtomicInteger(0);

    public DBImpl(String path){
        try {
            createDBPath(path);
        }catch (IOException e){
            System.out.println("create path error");
            e.printStackTrace();
        }
        this.valueLog = new ValueLog(path);

        //判断KeyLog文件是否存在,如果存在，进行内存恢复
        File dir = new File(path, "key");
        if (dir.exists()){
            System.out.println("---------------Start read or write append---------------");
            tmap = new TLongIntHashMap(32000000, 1.0F, -1L, -1);
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path);//keylog恢复
            recoverHashtable();//hashtable恢复和wroteposition恢复
            System.out.println("Recover finished");
        }

        //如果不存在，说明是第一次open
        else {
            System.out.println("---------------Start first write---------------");
            keyLog = new KeyLog(GlobalConfig.KeyFileSize, path);
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
        int wrotePosition = (int) (this.valueLog.getFileLength() / 4096);
        System.out.println(this.valueLog.getFileLength());
        System.out.println(wrotePosition);
        int size = wrotePosition;
        byte[] key = new byte[8];
        while (size > 0){
//            if (size % (1024*256) == 0)
//                System.out.println("recover   " + size);


            byteBuffer.get(key);

            tmap.put(ByteBuffer.wrap(key).getLong(), byteBuffer.getInt());
            size--;
        }
        this.valueLog.setWrotePosition(((long)wrotePosition)*4096);
        this.keyLog.setWrotePosition(wrotePosition * 12);
    }


//    public void write(byte[] key, byte[] value){
//        int position = valueLog.putMessageDirect(value);
//        keyLog.putKey(key, position, position*12);
//    }

    public void write(byte[] key, byte[] value){
        valueLog.putMessageDirect(key, value, this.keyLog);
    }

    public byte[] read(byte[] key) throws EngineException{
        int currentPos = tmap.get(ByteBuffer.wrap(key).getLong());

//        int now = readNum.getAndAdd(1);
//        if (now % 320000 == 0)
//            System.out.println(now);

        if (currentPos<0){

//            System.out.println("==========not found===========");
//            System.out.println(currentPos);
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
        }

        long value_file_wrotePosition = ((long)currentPos) * 4096;
        return valueLog.getMessageDirect(value_file_wrotePosition);
    }

    public void close(){
        keyLog.close();
        valueLog.close();
        keyLog = null;
        valueLog = null;
        tmap = null;
    }
}
