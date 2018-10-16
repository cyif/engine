package com.alibabacloud.polar_race.engine.common.config;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午1:50
 */
public class GlobalConfig {
    public static final String storePathRootDir = "alidata/";
    public static final String storePathValue = storePathRootDir + "value/";
    public static final String storePathKey = storePathRootDir + "key/";

    public static final int ValueFileSize = 1024 * 1024 * 1024;
    public static final long ValueFileNum = 64 * 1024 * 1024 * 4 * 1024 / ValueFileSize;
    public static final int KeyFileSize = 1024 * 1024 * 1024;

}
