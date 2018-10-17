package com.alibabacloud.polar_race.engine.common.utils;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午4:12
 */
public class ByteToInt {
    public static int byteArrayToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }


    //因为offset最多需要26位，所以28位是很足够的
    public static byte[] intToVarByteArray(int value){
        byte[] bytes = null;
        int highBitMask = 0x80;

        if (value < (1 << 7) && value >= 0) {
            bytes = new byte[1];
            bytes[0] = (byte) value;
        }
        else if (value < (1 << 14) && value > 0) {
            bytes = new byte[2];
            bytes[0] = (byte) (value | highBitMask);
            bytes[1] = (byte) (value >>> 7) ;
        }
        else if (value < (1 << 21) && value > 0) {
            bytes = new byte[3];
            bytes[0] = (byte) (value | highBitMask);
            bytes[1] = (byte) (value >>> 7 | highBitMask) ;
            bytes[2] = (byte) (value >>> 14) ;
        }
        else {
            bytes = new byte[4];
            bytes[0] = (byte) (value | highBitMask);
            bytes[1] = (byte) (value >>> 7 | highBitMask) ;
            bytes[2] = (byte) (value >>> 14 | highBitMask) ;
            bytes[3] = (byte) (value >>> 21) ;
        }
        return bytes;
    }
    public static int varByteArrayToInt(byte[] value){
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = value[shift/7];

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return result;
            }
        }

        return result;
    }
}
