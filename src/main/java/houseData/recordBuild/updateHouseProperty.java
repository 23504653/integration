package houseData.recordBuild;

import com.bean.HouseId;
import com.mapper.HouseIdMapper;
import com.utils.FindWorkBook;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class updateHouseProperty {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/updateHouseProperty.sql";
    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriter;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    public static void main(String agr[]) throws SQLException {
        projectBusinessFile = new File(PROJECT_FILE);
        if(projectBusinessFile.exists()){
            projectBusinessFile.delete();
        }

        try{
            projectBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(projectBusinessFile.getAbsoluteFile());
            projectBusinessWriter = new BufferedWriter(fw);
            projectBusinessWriter.write("USE record_building;");
            projectBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("projectBusinessWriter 文件创建失败");
            e.printStackTrace();
            return;
        }

        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        HouseId houseId = null;
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);

        try {
            houseResultSet = houseStatement.executeQuery("SELECT ID,CHANGE_HOUSE_TYPE FROM HOUSE_INFO.HOUSE");
            houseResultSet.last();
            int sumCount = houseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseResultSet.beforeFirst();
            while (houseResultSet.next()){
                houseId = houseIdMapper.selectByOldHouseId(houseResultSet.getString("ID"));
                if(houseId==null){
                    System.out.println("updateHouseProperty没有找到对应HOUSE_idE记录检查:--:"+houseResultSet.getString("ID"));
                    return;
                }
                workbookResultSet=workbookStatement.executeQuery("select * from record_building.house_snapshot where house_id="+houseId.getId());
                if (workbookResultSet.next()){
                    workbookResultSet.beforeFirst();
                    while (workbookResultSet.next()){
                        projectBusinessWriter.newLine();
                        String id = houseResultSet.getString("CHANGE_HOUSE_TYPE");
                        if(id==null || id.equals("")|| id.equals("COMM_USE_HOUSE")|| id.equals("SELF_CREATE")
                                || id.equals("STORE_HOUSE")){
                            projectBusinessWriter.write("update record_building.apartment_snapshot set house_property= null where apartment_info_id='"+workbookResultSet.getString("apartment_info_id")+"';");
                        }else {
                            projectBusinessWriter.write("update record_building.apartment_snapshot set house_property='" + FindWorkBook.houseProperty(houseResultSet.getString("CHANGE_HOUSE_TYPE")) + "' where apartment_info_id='" + workbookResultSet.getString("apartment_info_id") + "';");
                        }
                        projectBusinessWriter.flush();
                    }
                }
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }


    }
}
