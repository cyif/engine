package com.alibabacloud.polar_race.engine.common.config;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午1:50
 */
public class GlobalConfig {
//    public static final String storePathValue = "value";
//    public static final String storePathKey = "key";

    public static int kA = 54059;  // a prime
    public static int kB = 76963;  // another prime
    public static  int kFinish = 37;  // also prime

//    public static final int ValueFileSize = 1024 * 1024 * 1024;
//    public static final int ValueFileNum = (int)(((long)64 * 1024 * 1024 * 4 * 1024) / ValueFileSize);
//    public static final int KeyFileSize = 1024 * 1024 * 1024;

    public static final int ValueFileSize = 1073741824;
    public static final int ValueFileNum = 256;
    public static final int KeyFileSize = 1073741824;
}
