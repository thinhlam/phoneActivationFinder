package com.thinhl.utils;

import com.thinhl.model.PhoneActivationRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.Function;


public class Parser implements Function<Row, PhoneActivationRecord> {
    @Override
    public PhoneActivationRecord call(Row row) throws Exception {
        PhoneActivationRecord phoneActivationRecord = new PhoneActivationRecord();
        phoneActivationRecord.setPhoneNumber(row.getString(0));
        phoneActivationRecord.setActivationDate(row.getDate(1));
        phoneActivationRecord.setDeActivationDate(row.getDate(2));
        return phoneActivationRecord;
    }
}