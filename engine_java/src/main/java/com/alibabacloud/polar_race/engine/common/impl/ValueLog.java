package com.alibabacloud.polar_race.engine.common.impl;

import com.alibabacloud.polar_race.engine.common.utils.PutMessageLock;
import com.alibabacloud.polar_race.engine.common.utils.PutMessageReentrantLock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


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
    private ThreadLocal<ByteBuffer> threadLocal = ThreadLocal.withInitial(()->ByteBuffer.allocateDirect(4096));

    private PutMessageLock putMessageLock = new PutMessageReentrantLock();
//    private ThreadLocal<ByteBuffer> threadLocal = new ThreadLocal<>();
    public ValueLog(String storePath) {
        /*打开文件*/
        try {
            File file = new File(storePath, "value");
            if (!file.exists()){
                try {
                    file.createNewFile();
                }
                catch (IOException e){
                    System.out.println("Create file" + "valueLog" + "failed");
                    e.printStackTrace();
                }
            }
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + "valueLog" + " Failed. ");
        }
    }

    public long getFileLength(){
        try {
            return this.randomAccessFile.length();
        } catch (IOException e){
            e.printStackTrace();
        }
        return -1;
    }


    //返回第几个数据
    int putMessageDirect(byte[] value) {
//        if (threadLocal.get()==null)
//            threadLocal.set(ByteBuffer.allocateDirect(4096));
        ByteBuffer byteBuffer = threadLocal.get();
        byteBuffer.clear();
        byteBuffer.put(value);
        byteBuffer.flip();
        return channelWrite(byteBuffer);
    }

    private int channelWrite(ByteBuffer byteBuffer){
        long position = 0;
        putMessageLock.lock();
        try {
            position = this.fileChannel.position();
            this.fileChannel.write(byteBuffer);
        } catch (IOException e){
            e.printStackTrace();
        }
        putMessageLock.unlock();
        return (int) (position / 4096);
    }

    void setWrotePosition(long wrotePosition){
        try {
            this.fileChannel.position(wrotePosition);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    byte[] getMessageDirect(long offset) {
//        if (threadLocal.get()==null)
//            threadLocal.set(ByteBuffer.allocateDirect(4096));
        ByteBuffer byteBuffer = threadLocal.get();
        byteBuffer.clear();
        try {
            fileChannel.read(byteBuffer, offset);
        } catch (IOException e){
            e.printStackTrace();
        }
        byte[] bytes = new byte[4096];
        byteBuffer.flip();
        byteBuffer.get(bytes);
        return bytes;
    }
}
