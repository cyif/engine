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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午3:14
 */
public class DBImpl {
    private ValueLog valueLog[];
    //线程号与哪个value的对应
    private ConcurrentHashMap<Long, Integer> threadValueLog;
    private AtomicInteger whichValueLog = new AtomicInteger(0);

    private KeyLog keyLog;
    private AtomicInteger kelogWrotePosition = new AtomicInteger(0);

    private TLongIntHashMap tmap;

    private ThreadLocal<ByteBuffer> threadLocalReadBuffer = ThreadLocal.withInitial(()->ByteBuffer.allocateDirect(4096));
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(()->new byte[4096]);

    public DBImpl(String path){
        try {
            createDBPath(path);
        }catch (IOException e){
            System.out.println("create path error");
            e.printStackTrace();
        }

        this.threadValueLog = new ConcurrentHashMap<>(64);
        //创建64个value文件，分别命名value0--63
        this.valueLog = new ValueLog[64];
        for (int i=0; i<64; i++){
            valueLog[i] = new ValueLog(path, i);
        }



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
        int sum = 0;
        int[] valueLogWroteposition = new int[64];
        for (int i=0; i<64; i++){
            valueLogWroteposition[i] = (int)(valueLog[i].getFileLength() / 4096);
            sum += valueLogWroteposition[i];
        }
        System.out.println(sum);
        byte[] key = new byte[8];
        while (sum > 0){

            byteBuffer.get(key);

            tmap.put(ByteBuffer.wrap(key).getLong(), byteBuffer.getInt());
            sum--;
        }
        for (int i=0; i<64; i++){
            valueLog[i].setWrotePosition(((long)valueLogWroteposition[i])*4096);
        }
        this.keyLog.setWrotePosition(sum * 12);
    }


    public void write(byte[] key, byte[] value){

        long id = Thread.currentThread().getId();

        if (!threadValueLog.containsKey(id))
            threadValueLog.put(id, whichValueLog.getAndAdd(1));
        int valueLogNo = threadValueLog.get(id);

        //每个valuelog100w个数据，这个只占三个字节，表示该valuelog第几个数据
        int num = (int)(valueLog[valueLogNo].getWrotePosition() / 4096);
        //offset 第一个字节 表示这个key对应的存在哪个valuelog中，后三个字节表示这个value是该valuelog的第几个数据
        int offset = num | (valueLogNo<<24);

        //因为只用一个keylog，所以要有个原子量记录写在keylog中的位置
        keyLog.putKey(key, offset, kelogWrotePosition.getAndAdd(12));

        valueLog[valueLogNo].putMessageDirect(value);
    }

    public byte[] read(byte[] key) throws EngineException{
        int currentPos = tmap.get(ByteBuffer.wrap(key).getLong());
        if (currentPos<0){
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
        }
        int valueLogNo = currentPos >> 24;
        int num = (currentPos << 8) >> 8;

        long value_file_wrotePosition = ((long)num) * 4096;
        return valueLog[valueLogNo].getMessageDirect(value_file_wrotePosition, threadLocalReadBuffer.get(), threadLocalReadBytes.get());
    }

    public void close(){
        keyLog.close();
        for (ValueLog V : valueLog){
            V.close();
        }
        keyLog = null;
        valueLog = null;
        tmap = null;
    }
}
