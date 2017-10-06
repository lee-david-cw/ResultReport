package edu.pcc.fbj.rankingsystem.resultreporting;

import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.util.*;

/**
 * @author David Li
 * @version 2016.05.19
 */
public class StatisticsFirstChoiceReportDB  extends ReportDB implements ReportDAO
{
    /**
     *  Firstchoice statistics Constructor
     */
    public StatisticsFirstChoiceReportDB()
    {

    }

    /**
     * Retrieve user test result according to user's email
     *
     * @param   param HashMap<String, Object> param
     * @return List<ReportTestResult>
     */
    @Override
    public Object[][] getUserTestResult(HashMap<String, Object> param)
    {
        ArrayList<String> items = new ArrayList<>();
        ArrayList<String> percents = new ArrayList<>();

        if(param == null)
        {
            ReportLogger.LOGGER.severe("Parameter is null.");
            return null;
        }

        setMessage(DATABASE_DATA_READING);
        try (
                CallableStatement stmt = connection.prepareCall("{call FBJ_STATISTICS_ONE_SP(?)}",
                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)
        ) {
            stmt.setString(1, (String)param.get("TestName"));
            stmt.execute();
            ResultSet rs = stmt.getResultSet();
            while (rs.next())
            {
                items.add(rs.getString("Item"));
                percents.add(rs.getString("Result"));
            }

            Object[][] statisticsResults = new Object[items.size()][2];
            for(int i=0; i<items.size(); i++)
            {
                statisticsResults[i][0] = items.get(i);
                statisticsResults[i][1] = percents.get(i);
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
     * Retrieve column for statistics report
     * @param    param HashMap<String, Object> param
     * @return   List<String>
     */
    @Override
    public List<String> getUserTestTableColumns(HashMap<String, Object> param)
    {
        List<String> statisticsTableColumn =  new ArrayList<>();

        statisticsTableColumn.add("Item");
        statisticsTableColumn.add("Percent");

        return statisticsTableColumn;
    }
}
