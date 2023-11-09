package com.utils;

import com.bean.FloorBeginEnd;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 通过IN_FLOOR_NAME判断楼层的起始层，终止层
 *
 */
public class FindFloorBeginEnd {

    public static FloorBeginEnd changeFloorName(String floorName){
        FloorBeginEnd floorBeginEnd = new FloorBeginEnd();
        String pattern = "\\b\\d+[A-Za-z]*\\b"; // 匹配数字后面可以跟字母的组合
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(floorName);


        return floorBeginEnd;
    }


    public static void main(String[] args) {

        String input = "-1F-23F";
        String pattern = "\\d+"; //匹配一个或多个数字

        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(input);
        ArrayList<String> numbers = new ArrayList<String>();
        System.out.println("从字符串 '" + input + "' 中提取的数字部分:");

        while (m.find()) {
            String result = m.group(0);
            numbers.add(result);
        }

//       System.out.println(containsNoDigits(""));

           System.out.println("11-"+numbers.get(0));
           System.out.println("222-"+numbers.get(1));



    }
}
