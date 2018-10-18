package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.impl.DBImpl;

public class EngineRace extends AbstractEngine {

	DBImpl db;

	@Override
	public void open(String path) throws EngineException {
		db = new DBImpl(path);
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		db.write(key, value);
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		byte[] value = null;
		value = db.read(key);
		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
	}
	
	@Override
	public void close() {
		db = null;
	}

}
