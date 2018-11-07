package com.alibabacloud.polar_race.engine.common.impl;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.ByteToLong;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageLock;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageSpinLock;
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


    /*  仅用一个keylog文件  */
    private KeyLog keyLog;
    //因为只用了一个keylog文件，记录位置
    private AtomicInteger kelogWrotePosition = new AtomicInteger(0);


    /*  仅new一次异常  */
    private EngineException engineException;


    /*  内存恢复hash   */
    private LongIntHashMap hmap[];



    /*  用于读，增加读取并发性，减小gc    */
    private ThreadLocal<ByteBuffer> threadLocalReadBuffer = ThreadLocal.withInitial(()->ByteBuffer.allocateDirect(4096));
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(()->new byte[4096]);
    private Set<byte[]> set;

    public DBImpl(String path){
        try {
            createDBPath(path);
        }catch (IOException e){
            System.out.println("create path error");
            e.printStackTrace();
        }

        //创建64个value文件，分别命名value0--63
        this.valueLog = new ValueLog[256];
        for (int i=0; i<256; i++){
            valueLog[i] = new ValueLog(path, i);
        }
        //判断KeyLog文件是否存在,如果存在，说明之前写过数据，进行内存恢复
        File dir = new File(path, "key");
        if (dir.exists()){
//            System.out.println("---------------Start read or write append---------------");
//            hmap = new LongIntHashMap(64000000, 0.99);
            hmap = new LongIntHashMap[256];
            for (int i=0; i<256; i++){
                hmap[i] = new LongIntHashMap(250000, 0.99);
            }
            keyLog = new KeyLog(12 * 64 * 1024 * 1024, path);//keylog恢复
            this.engineException = new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
            this.set = ConcurrentHashMap.<byte[]> newKeySet();
            recoverHashtable();//hashtable恢复和wroteposition恢复
//            System.out.println("Recover finished");
        }

        //如果不存在，说明是第一次open
        else {
//            System.out.println("---------------Start first write---------------");
            keyLog = new KeyLog(12 * 64 * 1024 * 1024, path);
        }
    }

    private void createDBPath(String dbPath) throws IOException {
        Path path = Paths.get(dbPath);
        if (!Files.exists(path))
            Files.createDirectory(path);
    }

    private void recoverHashtable(){
        ByteBuffer byteBuffer = keyLog.getKeyBuffer();
        byteBuffer.position(0);
        int sum = 0;//总共写入了多少个数据
        for (int i=0; i<256; i++){
            valueLog[i].setNum((int)(valueLog[i].getFileLength() >> 12));
            sum += valueLog[i].getNum();
        }

        byte[] key = new byte[8];
        while (sum > 0){
            byteBuffer.get(key);
            hmap[(int)(key[0]&0xff)].put(ByteToLong.byteArrayToLong(key), byteBuffer.getInt());
//            hmap.put(ByteToLong.byteArrayToLong(key), byteBuffer.getInt());
            sum--;
        }

        //恢复valuelog以及keylog写的位置，恢复到末尾
        for (int i=0; i<256; i++){
            valueLog[i].setWrotePosition(((long)valueLog[i].getNum()) << 12);
        }
        this.keyLog.setWrotePosition(sum * 12);
    }


    public void write(byte[] key, byte[] value){
        valueLog[(int)(key[0]&0xff)].putMessageDirect(value , keyLog, key, kelogWrotePosition);
    }

    public byte[] read(byte[] key) throws EngineException{

        if (set.contains(key)){
            throw this.engineException;
        }

        int currentPos = hmap[(int)(key[0]&0xff)].getOrDefault(ByteToLong.byteArrayToLong(key), -1);
        if (currentPos==-1){
            set.add(key);
            throw this.engineException;
        }
        return valueLog[(int)(key[0]&0xff)].getMessageDirect(((long)currentPos) << 12, threadLocalReadBuffer.get(), threadLocalReadBytes.get());
    }

    public void close(){
        keyLog.close();
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
