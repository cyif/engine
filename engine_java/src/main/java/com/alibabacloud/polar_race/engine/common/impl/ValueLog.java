package com.alibabacloud.polar_race.engine.common.impl;




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
    private ByteBuffer byteBuffer;


    public ValueLog(String storePath) {
        this.byteBuffer = ByteBuffer.allocateDirect(4096);
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


    void putMessage(byte[] value, long wrotePosition) {
        this.byteBuffer.clear();
        this.byteBuffer.put(value);
        this.byteBuffer.flip();
        try {
            this.fileChannel.write(this.byteBuffer, wrotePosition);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    void setWrotePosition(long wrotePosition){
        try {
            this.fileChannel.position(wrotePosition);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
    void putMessage(byte[] value) {
        this.byteBuffer.clear();
        this.byteBuffer.put(value);
        this.byteBuffer.flip();
        try {
            this.fileChannel.write(this.byteBuffer);
        } catch (IOException e){
            e.printStackTrace();
        }
    }


    byte[] getMessage(long offset) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
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
