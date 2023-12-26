package com.mapper.build;

import com.bean.build.BizId;

import java.util.Map;

public interface BizIdMapper {
    BizId selectByOldId(String businessId);
    int addBizId(Map<String, Object> map);
    Integer findMaxId();
}
