package com.mapper;

import com.bean.WorkBook;

import java.util.Map;

public interface WorkBookMapper {
    public WorkBook selectByOldId(String workId);
    public int addWorkBook(Map<String,Object> map);
    public Integer findMaxId();
}
