package com.alibabacloud.polar_race.engine.common.impl;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午1:43
 */
public class ValueLog {

    /*文件的大小,最好是1G*/
//    private final int FileSize;

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;

    public ValueLog(int FileSize, String storePath, int fileName) {
//        this.FileSize = FileSize;
        /*检查文件夹是否存在*/
//        ensureDirOK(storePath);
        /*打开文件*/
        try {
            File file = new File(storePath, String.valueOf(fileName));
            if (!file.exists()){
                try {
                    file.createNewFile();
                }
                catch (IOException e){
                    System.out.println("Create file" + fileName + "failed");
                    e.printStackTrace();
                }
            }
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);

        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + fileName + " Failed. ");
        } catch (IOException e) {
            System.out.println("map file " + 0 + " Failed. ");
        }
    }



    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                System.out.println(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    //mappedbytebuffer写一个数据
    void putMessage(byte[] value, int wrotePosition) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(wrotePosition);
        byteBuffer.put(value);
    }

    //mappedbytebuffer读取数据
    byte[] getMessage(int offset) {
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(offset);
        byte[] bytes = new byte[4096];
        byteBuffer.get(bytes);
        return bytes;
    }


}
