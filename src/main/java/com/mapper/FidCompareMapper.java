package com.mapper;

import com.bean.FidCompare;

import java.util.Map;


public interface FidCompareMapper {

    FidCompare selectOne(String oldId);

    int addFidCompare(Map<String,Object> map);


}
