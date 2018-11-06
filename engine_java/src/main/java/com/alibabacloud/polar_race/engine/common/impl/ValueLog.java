package com.alibabacloud.polar_race.engine.common.impl;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
//import static com.alibabacloud.polar_race.engine.common.utils.UnsafeUtil.UNSAFE;

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

    private ByteBuffer directWriteBuffer;

    private int num;

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
            this.num = 0;
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + "valueLog" + " Failed. ");
        }

        this.directWriteBuffer = ByteBuffer.allocateDirect(4096);
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
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
        num++;
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
        directWriteBuffer = null;
        try {
            this.fileChannel.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
