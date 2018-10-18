package com.alibabacloud.polar_race.engine.common.utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 上午10:35
 */
public class PutHashSpinLock implements PutHashLock{
    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    public void lock() {
        boolean flag;
        do {
            flag = this.putMessageSpinLock.compareAndSet(true, false);
        }
        while (!flag);
    }


    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }
}
