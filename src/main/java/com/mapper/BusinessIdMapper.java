package com.mapper;

import com.bean.BusinessId;

import java.util.Map;

public interface BusinessIdMapper {
    BusinessId selectByOldBusinessId(String BusinessOldId);
    int addBusinessId(Map<String,Object> map);
    Integer findMaxId();
}
