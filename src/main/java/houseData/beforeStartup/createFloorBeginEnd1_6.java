package houseData.beforeStartup;

import com.bean.FloorBeginEnd;
import com.bean.HouseUseType;
import com.mapper.FloorBeginEndMapper;
import com.mapper.HouseUseTypeMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class createFloorBeginEnd1_6 {



    public static boolean containsNoDigits(String inputStr) {
        // 使用正则表达式判断是否不包含数字
        String pattern = "^[^\\d]*$";
        return inputStr.matches(pattern);
    }

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement;
    private static ResultSet resultSet;
    public static void main(String agr[]) throws SQLException {
        statement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        FloorBeginEndMapper floorBeginEndMapper = sqlSession.getMapper(FloorBeginEndMapper.class);
        FloorBeginEnd floorBeginEnd = null;
        ArrayList<String> numbers = new ArrayList<String>();
        int upf=0,downf=0;
        try{
            //List<HouseUseType> houseUseTypeList = houseUseTypeMapper.findAll();
            String pattern = "\\d+"; //匹配一个或多个数字
            Pattern r = Pattern.compile(pattern);

            Map<String,Object> map = new HashMap<>(); // WHERE HOUSE.IN_FLOOR_NAME IN('-1F-23F','-1F','-1F至6F')
            resultSet = statement.executeQuery("SELECT IN_FLOOR_NAME FROM HOUSE_INFO.HOUSE GROUP BY IN_FLOOR_NAME order by IN_FLOOR_NAME");
            resultSet.last();
            int i=0,sumCount = resultSet.getRow();
            resultSet.beforeFirst();
            upf=1;
            downf=1;
            while (resultSet.next()){
                numbers.clear();
                map.clear();
                floorBeginEnd = floorBeginEndMapper.selectByName(resultSet.getString("IN_FLOOR_NAME"));

                if (floorBeginEnd == null){

                    map.put("name",resultSet.getString("IN_FLOOR_NAME"));
                    if (resultSet.getString("IN_FLOOR_NAME")!=null && !resultSet.getString("IN_FLOOR_NAME").equals("")){
                        Matcher m = r.matcher(resultSet.getString("IN_FLOOR_NAME"));
                        while (m.find()) {
                            String result = m.group(0);
                            numbers.add(result);
                        }
                        if(numbers.size()>0 && numbers.get(0)!=null && !numbers.get(0).equals("")) {
                            upf = Integer.valueOf(numbers.get(0));
                        }else{
                            upf=1;
                        }

                        if(numbers.size()>1 && numbers.get(1)!=null && !numbers.get(1).equals("")) {
                            downf = Integer.valueOf(numbers.get(1));
                        }else{
                            downf = 1;
                        }

                        if (downf==1){
                            downf = upf;
                        }
                        if (resultSet.getString("IN_FLOOR_NAME").indexOf("阁楼")>=0 || resultSet.getString("IN_FLOOR_NAME").indexOf("闷顶")>=0 ){
                            downf = downf+1;

                        }




                        map.put("upf",upf);
                        map.put("downf",downf);
                        System.out.println("IN_FLOOR_NAME--:"+map.get("name")+"---:"+map.get("upf")+"----:"+map.get("downf"));

                        floorBeginEndMapper.addFloorBeginEnd(map);

                        System.out.println("add 记录 IN_FLOOR_NAME--"+resultSet.getString("IN_FLOOR_NAME"));
                        sqlSession.commit();

                    }


                }
                i++;
                System.out.println("Count --"+sumCount+"/"+i);
            }



        }catch (Exception e){
            e.printStackTrace();
            return;
        }finally {
            sqlSession.close();
            if(resultSet!=null){
                resultSet.close();
            }
            if(statement!=null){
                statement.close();
            }
            MyConnection.closeConnection();


        }
    }
}
