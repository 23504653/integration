package houseData.recordBuild;

import com.bean.BusinessId;
import com.bean.LandEndTimeId;
import com.bean.ProjectId;
import com.mapper.BusinessIdMapper;
import com.mapper.LandEndTimeIdMapper;
import com.mapper.ProjectIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import com.utils.Q;
import com.utils.FindWorkBook;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class projectBuildMain1 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static final String PROJECT_ERROR_FILE="/projectError1.sql";
    private static final String PROJECT_FILE="/projectRecord1.sql";
    private static BufferedWriter projectWriterError;
    private static BufferedWriter projectWriter;
    private static File projectFileError;
    private static File projectFile;
    private static Statement projectStatement;
    private static Statement businessStatement;
    private static ResultSet projectResultSet;
    private static ResultSet businessResultSet;
    private static Statement landEndTimeStatement;
    private static ResultSet landEndTimeResultSet;
    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;


    public static void main(String agr[]) throws SQLException {

        projectFileError = new File(PROJECT_ERROR_FILE);
        if(projectFileError.exists()){
            projectFileError.delete();
        }
        projectFile = new File(PROJECT_FILE);
        if(projectFile.exists()){
            projectFile.delete();
        }

        try{
            projectFile.createNewFile();
            FileWriter fw = new FileWriter(projectFile.getAbsoluteFile());
            projectWriter = new BufferedWriter(fw);
            projectWriter.write("USE record_building;");
            projectWriter.newLine();
            projectWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_project_import','预销售许可证业务导入',false,true,0,'data');");
            projectWriter.newLine();
            projectWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) "
                    +"value (1,'OLD','2023-10-28 18:30:45','2023-10-28 18:30:45','预销售许可证业务导入','COMPLETED','2023-10-28 18:30:45','2023-10-28 18:30:45',0,'process_project_import',false,'data');");
            projectWriter.newLine();
            projectWriter.write("INSERT work_operator (work_id, type, user_id, user_name, org_name, task_id) VALUE (1,'CREATE','0','root','预销售许可证业务导入','wxy_project');");
            projectWriter.newLine();
            projectWriter.write("INSERT work_task (TASK_ID, MESSAGE, TASK_NAME, PASS) VALUE ('wxy_project','预销售许可证业务导入','预销售许可证业务导入',true);");
            projectWriter.flush();
        }catch (IOException e){
            System.out.println("projectWriter 文件创建失败");
            e.printStackTrace();
            return;
        }


        try{
            projectFileError.createNewFile();
            FileWriter fw = new FileWriter(projectFileError.getAbsoluteFile());
            projectWriterError = new BufferedWriter(fw);
            projectWriterError.write("project--错误记录:");
            projectWriterError.newLine();
            projectWriterError.flush();
        }catch (IOException e){
            System.out.println("projectWriterError 文件创建失败");
            e.printStackTrace();
            return;
        }
        projectStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        businessStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        landEndTimeStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        workbookStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        LandEndTimeIdMapper landEndTimeIdMapper = sqlSession.getMapper(LandEndTimeIdMapper.class);
        BusinessIdMapper businessIdMapper = sqlSession.getMapper(BusinessIdMapper.class);
        ProjectId projectId = null;
        LandEndTimeId landEndTimeId= null;
        BusinessId businessId = null;
        String busId,pId=null;

        try {
//            projectResultSet = projectStatement.executeQuery("SELECT * FROM HOUSE_INFO.PROJECT ORDER BY NAME");
            projectResultSet = projectStatement.executeQuery("SELECT * FROM HOUSE_INFO.PROJECT WHERE ID='127' ORDER BY NAME");
            projectResultSet.last();
            int sumCount = projectResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            projectResultSet.beforeFirst();
            while (projectResultSet.next()){

                //在码表找到对应的新ID
                projectId = projectIdMapper.selectByOldProjectId(projectResultSet.getString("ID"));

                if(projectId == null){
                    projectWriterError.newLine();
                    projectWriterError.write("没有找到对应记录检查jproject_Id--:"+projectResultSet.getString("ID"));
                    projectWriterError.flush();
                    return;
                }
                businessResultSet = businessStatement.executeQuery("SELECT P.ID AS PID,P.*,PI.*,O.* FROM HOUSE_OWNER_RECORD.PROJECT AS P "
                +"LEFT JOIN HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PI ON P.ID = PI.ID LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID "
                +"WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING','RUNNING') AND DEFINE_ID='WP50' "
//                +"AND P.PROJECT_CODE='127' "
                +"AND P.PROJECT_CODE='"+projectResultSet.getString("ID")+"' "
                +"ORDER BY P.NAME,O.ID,O.APPLY_TIME;");

                if(businessResultSet.next()){//项目办理过预售业务的
                    businessResultSet.beforeFirst();
                    while (businessResultSet.next()) {
                        businessId = businessIdMapper.selectByOldBusinessId(businessResultSet.getString("BUSINESS"));
                        if (businessId ==null){
                            projectWriterError.newLine();
                            projectWriterError.write("没有找到对应记录检查businessId:"+businessResultSet.getString("ID"));
                            projectWriterError.flush();
                            System.out.println("没有找到对应记录检查businessId:"+businessResultSet.getString("ID"));
                            return;
                        }
                        //land_snapshot
                        projectWriter.newLine();
                        projectWriter.write("INSERT record_building.land_snapshot (CAPARCEL_NUMBER, LAND_NUMBER, PROPERTY, BEGIN_DATE, TAKE_TYPE_KEY, TAKE_TYPE, AREA, ADDRESS, LICENSE_NUMBER, LICENSE_TYPE, LICENSE_TYPE_KEY, LAND_INFO_ID) VALUE ");
                        projectWriter.write("(" + Q.v(Q.pm("未知"),Q.pm(businessResultSet.getString("NUMBER"))
                                ,Q.p(FindWorkBook.changeLandProperty(businessResultSet.getString("LAND_PROPERTY")).getValue()),Q.pm(businessResultSet.getTimestamp("BEGIN_USE_TIME"))
                                ,Q.pm(FindWorkBook.changeLandTakeType(businessResultSet.getString("LAND_GET_MODE")).getId()),Q.p(FindWorkBook.changeLandTakeType(businessResultSet.getString("LAND_GET_MODE")).getValue())
                                ,Q.pm(businessResultSet.getBigDecimal("LAND_AREA")),Q.pm(businessResultSet.getString("LAND_ADDRESS"))
                                ,Q.pm(businessResultSet.getString("LAND_CARD_NO")),Q.pm(FindWorkBook.changeLandCardType(businessResultSet.getString("LAND_CARD_TYPE")).getValue())
                                ,Q.pm(FindWorkBook.changeLandCardType(businessResultSet.getString("LAND_CARD_TYPE")).getId())
                                ,Long.toString(businessId.getId())
                        )+ ");");
                        projectWriter.flush();



                        //land_use_type_snapshot
                        landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.LAND_END_TIME WHERE PROJECT_ID='"+businessResultSet.getString("PID")+"'");
                        if(landEndTimeResultSet.next()){
                            landEndTimeResultSet.beforeFirst();
                            while (landEndTimeResultSet.next()){
                                landEndTimeId = landEndTimeIdMapper.selectByOldId(landEndTimeResultSet.getString("ID"));
                                if(landEndTimeId!=null && landEndTimeId.getOid()!=null && !landEndTimeId.getOid().equals("")){
                                    projectWriter.newLine();
                                    projectWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                                    projectWriter.write("(" + Q.v(Q.pm(landEndTimeResultSet.getTimestamp("END_TIME")),Q.pm(landEndTimeResultSet.getString("USE_TYPE"))
                                            ,Long.toString(businessId.getId())
                                            ,Long.toString(landEndTimeId.getId())
                                    )+ ");");
                                    projectWriter.flush();
                                }else {
                                    projectWriterError.newLine();
                                    projectWriterError.write("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                    projectWriterError.flush();
                                    System.out.println("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                    return;
                                }


                            }
                        }





                    }
                }else{//项目没办理过预售业务的

                }




                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }







        }catch (Exception e){
            System.out.println("id is errer-----id:"+projectResultSet.getString("ID"));
            e.printStackTrace();
            return;

        }finally {

            sqlSession.close();

            if (projectResultSet!=null){
                projectResultSet.close();
            }
            if(projectStatement!=null){
                projectStatement.close();
            }

            if (businessResultSet!=null){
                businessResultSet.close();
            }
            if(businessStatement!=null){
                businessStatement.close();
            }
            MyConnection.closeConnection();
        }

    }
}
