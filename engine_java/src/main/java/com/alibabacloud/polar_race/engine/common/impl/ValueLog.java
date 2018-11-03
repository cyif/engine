package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.utils.PutMessageLock;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageReentrantLock;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Supplier;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午1:43
 */
public class ValueLog {

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    private RandomAccessFile randomAccessFile;
    //直接内存
//    private ThreadLocal<ByteBuffer> threadLocal = ThreadLocal.withInitial(()->ByteBuffer.allocateDirect(4096));
//    private ThreadLocal<byte[]> threadLocalBytes = ThreadLocal.withInitial(()->new byte[4096]);

    private ByteBuffer directWriteBuffer;

    private PutMessageLock putMessageLock;
//    private ThreadLocal<ByteBuffer> threadLocal = new ThreadLocal<>();
    public ValueLog(String storePath, int filename) {
        /*打开文件*/
        try {
            File file = new File(storePath, "value"+filename);
            if (!file.exists()){
                try {
                    file.createNewFile();
                }
                catch (IOException e){
                    System.out.println("Create file" + "valueLog" + filename + "failed");
                    e.printStackTrace();
                }
            }
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + "valueLog" + " Failed. ");
        }

        this.directWriteBuffer = ByteBuffer.allocateDirect(4096);
        this.putMessageLock = new PutMessageReentrantLock();
    }

    public long getFileLength(){
        try {
            return this.randomAccessFile.length();
        } catch (IOException e){
            e.printStackTrace();
        }
        return -1;
    }

    public void putMessageDirect(byte[] value) {
        this.directWriteBuffer.clear();
        this.directWriteBuffer.put(value);
        this.directWriteBuffer.flip();
        try {
            this.fileChannel.write(this.directWriteBuffer);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public long getWrotePosition(){
        try {
            return this.fileChannel.position();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return -1;
    }

    void setWrotePosition(long wrotePosition){
        try {
            this.fileChannel.position(wrotePosition);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    byte[] getMessageDirect(long offset, ByteBuffer byteBuffer, byte[] bytes) {

        byteBuffer.clear();
        try {
            fileChannel.read(byteBuffer, offset);
        } catch (IOException e){
            e.printStackTrace();
        }
        byteBuffer.flip();
        byteBuffer.get(bytes);
        return bytes;
    }


    public void close(){
        try {
            this.fileChannel.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
