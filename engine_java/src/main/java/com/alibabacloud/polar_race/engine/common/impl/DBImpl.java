package com.alibabacloud.polar_race.engine.common.impl;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.ByteToLong;
import com.carrotsearch.hppc.LongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午3:14
 */
public class DBImpl {

    /*  根据key的第一位划分valuelog  */
    private ValueLog valueLog[];


    /*  根据key的第一位划分keylog  */
    private KeyLog keyLog[];


    /*  仅new一次异常  */
    private EngineException engineException;


    /*  根据key的第一位划分，内存恢复hash   */
    private LongIntHashMap hmap[];


    /*  用于读，增加读取并发性，减小gc    */
    private ThreadLocal<ByteBuffer> threadLocalReadBuffer = ThreadLocal.withInitial(()->ByteBuffer.allocateDirect(4096));
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(()->new byte[4096]);
    //直接过滤掉不存在的key
    private Set<byte[]> set;

    public DBImpl(String path){
        try {
            createDBPath(path);
        }catch (IOException e){
            System.out.println("create path error");
            e.printStackTrace();
        }

        //创建256个value文件，分别命名value0--256
        this.valueLog = new ValueLog[256];
        for (int i=0; i<256; i++){
            valueLog[i] = new ValueLog(path, i);
        }
        //判断Key文件夹是否存在,如果存在，说明之前写过数据，进行内存恢复
        File dir = new File(path, "key");
        if (dir.exists()){
            System.out.println("---------------Start read or write append---------------");
            hmap = new LongIntHashMap[256];
            for (int i=0; i<256; i++){
                //这个值是250000/0.99的最小整数
                hmap[i] = new LongIntHashMap(253003, 0.99);
            }

            keyLog = new KeyLog[256];
            for (int i=0; i<256; i++){
                //根据日志，基本上每个都25w多一点点
                keyLog[i] = new KeyLog(12*252000, path + File.separator + "key", i);
            }

            this.engineException = new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
            this.set = ConcurrentHashMap.<byte[]> newKeySet();
            recoverHashtable();//hashtable恢复和wroteposition恢复
        }

        //如果不存在，说明是第一次open
        else {
            System.out.println("---------------Start first write---------------");
            keyLog = new KeyLog[256];
            for (int i=0; i<256; i++){
                //根据日志，基本上每个都25w多一点点
                keyLog[i] = new KeyLog(12*252000, path + File.separator + "key", i);
            }
        }
    }

    private void createDBPath(String dbPath) throws IOException {
        Path path = Paths.get(dbPath);
        if (!Files.exists(path))
            Files.createDirectory(path);
    }

    private void recoverHashtable(){

        AtomicInteger keylogRecoverNum = new AtomicInteger(0);
        Thread[] threads = new Thread[256];
        for (int i = 0; i < 256; i++) {
            threads[i] = new Thread(
                    () -> {
                        int logNum = keylogRecoverNum.getAndIncrement();
//
                        KeyLog keyLogi = keyLog[logNum];
                        ValueLog valueLogi = valueLog[logNum];
                        LongIntHashMap hmapi = hmap[logNum];

                        ByteBuffer byteBuffer = keyLogi.getKeyBuffer();
                        byteBuffer.position(0);

                        int sum = (int) (valueLogi.getFileLength() >> 12);

                        byte[] key = new byte[8];
                        for (int currentNum = 0; currentNum < sum; currentNum++) {
                            byteBuffer.get(key);
                            hmapi.put(ByteToLong.byteArrayToLong_seven(key), byteBuffer.getInt());
                        }

                        valueLogi.setNum(sum);
                        valueLogi.setWrotePosition(((long) sum) << 12);
                        keyLogi.setWrotePosition(sum * 12);

                    }
            );
        }
        for (int i = 0; i < 256; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 256; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public void write(byte[] key, byte[] value){
        int logNum = (int)(key[0]&0xff);
        valueLog[logNum].putMessageDirect(value , keyLog[logNum], key);
    }

    public byte[] read(byte[] key) throws EngineException{

        if (set.contains(key)){
            throw this.engineException;
        }

        int logNum = (int)(key[0]&0xff);
        int currentPos = hmap[logNum].getOrDefault(ByteToLong.byteArrayToLong_seven(key), -1);
        if (currentPos==-1){
            set.add(key);
            throw this.engineException;
        }
        return valueLog[logNum].getMessageDirect(((long)currentPos) << 12, threadLocalReadBuffer.get(), threadLocalReadBytes.get());
    }

    public void close(){
        for (KeyLog K : keyLog){
            K.close();
        }

        for (ValueLog V : valueLog){
            V.close();
        }
        keyLog = null;
        valueLog = null;
        hmap = null;
        threadLocalReadBuffer = null;
        threadLocalReadBytes = null;
    }
}
