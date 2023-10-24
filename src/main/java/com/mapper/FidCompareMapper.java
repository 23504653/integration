package com.mapper;

import com.bean.FidCompare;


public interface FidCompareMapper {

    FidCompare selectOne(String oldId);

    void addFidCompare(FidCompare fidCompare);


}
