package com.thinhl.utils;

import com.thinhl.model.PhoneActivationRecord;
import org.apache.commons.collections.IteratorUtils;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ActivationFinderUtils {

    public static String DATE_PATTERN = "yyyy-MM-dd";

    public static String findRealActivationDate(Iterator<PhoneActivationRecord> records) {
        if (records == null || !records.hasNext())
            return "";

        List<PhoneActivationRecord> recordLst = IteratorUtils.toList(records);
        recordLst.sort(Comparator.comparing(PhoneActivationRecord::getActivationDate));

        Date realActivationDate = recordLst.get(0) != null ? recordLst.get(0).getActivationDate() : null;
        for (int i = 0; i < recordLst.size(); i++) {
            if(i + 1 < recordLst.size()){
                if(!(recordLst.get(i).getDeActivationDate().getTime() == recordLst.get(i + 1).getActivationDate().getTime())){
                    realActivationDate = recordLst.get(i+1).getActivationDate();
                }
            }
        }
        return new SimpleDateFormat(DATE_PATTERN).format(realActivationDate);

    }
}
