package com.alibabacloud.polar_race.engine.common.impl;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.utils.ByteToLong;
import com.carrotsearch.hppc.LongIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午3:14
 */
public class DBImpl {

    /*  每个线程对应一个valuelog文件  */
    private ValueLog valueLog[];
    //线程号与valuelog文件的对应
    private ConcurrentHashMap<Long, Integer> threadValueLog;
    private AtomicInteger whichValueLog = new AtomicInteger(0);

    /*  仅用一个keylog文件  */
    private KeyLog keyLog;
    //因为只用了一个keylog文件，记录位置
    private AtomicInteger kelogWrotePosition = new AtomicInteger(0);

    /*  内存恢复hash   */
//    private TLongIntHashMap tmap;
    private LongIntHashMap hmap;

    /*  用于读，增加读取并发性，减小gc    */
    private ThreadLocal<ByteBuffer> threadLocalReadBuffer = ThreadLocal.withInitial(()->ByteBuffer.allocateDirect(4096));
    private ThreadLocal<byte[]> threadLocalReadBytes = ThreadLocal.withInitial(()->new byte[4096]);

    public DBImpl(String path){
        try {
            createDBPath(path);
        }catch (IOException e){
            System.out.println("create path error");
            e.printStackTrace();
        }

        this.threadValueLog = new ConcurrentHashMap<>(128);
        //创建64个value文件，分别命名value0--63
        this.valueLog = new ValueLog[64];
        for (int i=0; i<64; i++){
            valueLog[i] = new ValueLog(path, i);
        }
        //判断KeyLog文件是否存在,如果存在，说明之前写过数据，进行内存恢复
        File dir = new File(path, "key");
        if (dir.exists()){
//            System.out.println("---------------Start read or write append---------------");
            //如果找不到key就会返回-1
//            tmap = new TLongIntHashMap(64 * 1024 * 1024, 1.0F, 0L, -1);
            hmap = new LongIntHashMap(64 * 1000 * 1000, 0.99);
            keyLog = new KeyLog(12 * 64 * 1024 * 1024, path);//keylog恢复
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
        int[] valueLogWroteposition = new int[64];//每个valuelog文件写入了多少个数据
        for (int i=0; i<64; i++){
            valueLogWroteposition[i] = (int)(valueLog[i].getFileLength() / 4096);
            sum += valueLogWroteposition[i];
        }
//        System.out.println(sum);
        byte[] key = new byte[8];
        while (sum > 0){
            byteBuffer.get(key);
//            tmap.put(ByteToLong.byteArrayToLong(key), byteBuffer.getInt());
            hmap.put(ByteToLong.byteArrayToLong(key), byteBuffer.getInt());
            sum--;
        }

        //恢复valuelog以及keylog写的位置，恢复到末尾
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
//        int currentPos = tmap.get(ByteToLong.byteArrayToLong(key));

        int currentPos = hmap.getOrDefault(ByteToLong.byteArrayToLong(key), -1);
        if (currentPos==-1){
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found this key");
        }
//        int valueLogNo = currentPos >> 24;
//        int num = currentPos & 0x00FFFFFF;
//        long value_file_wrotePosition = ((long)num) * 4096;
        return valueLog[currentPos >> 24].getMessageDirect(((long)(currentPos & 0x00FFFFFF)) * 4096, threadLocalReadBuffer.get(), threadLocalReadBytes.get());
    }

    public void close(){
        keyLog.close();
        for (ValueLog V : valueLog){
            V.close();
        }
        keyLog = null;
        valueLog = null;
//        tmap = null;
        hmap = null;
        threadLocalReadBuffer = null;
        threadLocalReadBytes = null;
        threadValueLog = null;
    }
}
