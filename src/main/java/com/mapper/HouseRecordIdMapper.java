package com.mapper;

import com.bean.HouseId;
import com.bean.HouseRecordId;

import java.util.Map;

public interface HouseRecordIdMapper {

    HouseRecordId selectByOid(String oid);
    int addHouseRecordId(Map<String,Object> map);
    Integer findMaxId();
}
