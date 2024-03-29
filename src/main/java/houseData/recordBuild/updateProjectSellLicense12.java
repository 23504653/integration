package houseData.recordBuild;

import com.bean.OwnerRecordProjectId;
import com.mapper.OwnerRecordProjectIdMapper;
import com.mapper.ProjectIdMapper;
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

public class updateProjectSellLicense12 {

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/updateProjectSellLicense.sql";
    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriter;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

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

        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper = sqlSession.getMapper(OwnerRecordProjectIdMapper.class);
        OwnerRecordProjectId ownerRecordProjectId = null;
        try {
            workbookResultSet = workbookStatement.executeQuery("SELECT MA.*,PC.*,P.ID AS PID,CAST(SUBSTRING(MA.NUMBER, 7) AS SIGNED) AS on_number,NUMBER FROM HOUSE_OWNER_RECORD.PROJECT_CARD AS PC LEFT JOIN HOUSE_OWNER_RECORD.MAKE_CARD AS MA ON PC.ID = MA.ID " +
                    "LEFT  JOIN  PROJECT AS P ON PC.PROJECT= P.ID WHERE MA.TYPE='PROJECT_RSHIP' ORDER BY PRINT_TIME");
            workbookResultSet.last();
            int sumCount = workbookResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            workbookResultSet.beforeFirst();
            while (workbookResultSet.next()){
                int year = 0,cardNmuber=0;
                ownerRecordProjectId = ownerRecordProjectIdMapper.selectByOldId(workbookResultSet.getString("PID"));
                if(ownerRecordProjectId == null){
                    System.out.println("没有找到对应记录检查ownerRecordProjectId--:"+workbookResultSet.getString("PID"));
                    return;
                }
                if (workbookResultSet.getString("NUMBER")!=null
                        && !workbookResultSet.getString("NUMBER").isBlank()){
                    year = FindWorkBook.getYearMonthFromDate(workbookResultSet.getTimestamp("PRINT_TIME"));
                    cardNmuber = workbookResultSet.getInt("on_number");

                }
                projectBusinessWriter.newLine();
                projectBusinessWriter.write("update project_sell_license set year_number = '"+year+"',on_number ='"+cardNmuber+"' WHERE license_id='" + ownerRecordProjectId.getId() + "';");

                projectBusinessWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){

        }finally {

        }


    }
}
