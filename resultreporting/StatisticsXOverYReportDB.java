package edu.pcc.fbj.rankingsystem.resultreporting;

import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author David Li
 * @version 2016.05.30
 */
public class StatisticsXOverYReportDB extends ReportDB implements ReportDAO
{

    /**
     * Constructor
     */
    public StatisticsXOverYReportDB()
    {

    }

    /**
     * Retrieve user test result according to user's email
     * @param   param HashMap<String, Object> param
     * @return List<ReportTestResult>
     */
    @Override
    public Object[][] getUserTestResult(HashMap<String, Object> param)
    {
        if(param == null)
        {
            ReportLogger.LOGGER.severe("Parameter is null.");
            return null;
        }

        ArrayList<String> itemX = new ArrayList<>();
        ArrayList<String> itemY = new ArrayList<>();
        ArrayList<String> percents = new ArrayList<>();

        setMessage(DATABASE_DATA_READING);
        try (
                CallableStatement stmt = connection.prepareCall("{call FBJ_STATISTICS_XOVERY_SP(?)}",
                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)
        ) {
            stmt.setString(1, (String)param.get("TestName"));
            stmt.execute();
            ResultSet rs = stmt.getResultSet();
            while (rs.next())
            {
                itemX.add(rs.getString("ItemX"));
                itemY.add(rs.getString("ItemY"));
                percents.add(rs.getString("Result"));
            }

            Object[][] statisticsResults = new Object[itemX.size()][3];
            for(int i=0; i<itemX.size(); i++)
            {
                statisticsResults[i][0] = itemX.get(i);
                statisticsResults[i][1] = itemY.get(i);
                statisticsResults[i][2] = percents.get(i);
            }
            rs.close();
            setMessage(DATABASE_DATA_COMPLETE);
            return statisticsResults;
        }
        catch (SQLException se)
        {
            ReportLogger.LOGGER.severe("Cannot get email list due to database exception.");
            return null;
        }
    }

    /**
     * Retrieve colomn for report
     *
     * @param    param HashMap<String, Object> param
     * @return   List<String>
     */
    @Override
    public List<String> getUserTestTableColumns(HashMap<String, Object> param)
    {
        List<String> statisticsTableColumn =  new ArrayList<>();

        statisticsTableColumn.add("ItemX");
        statisticsTableColumn.add("ItemY");
        statisticsTableColumn.add("Percent");

        return statisticsTableColumn;
    }


}
