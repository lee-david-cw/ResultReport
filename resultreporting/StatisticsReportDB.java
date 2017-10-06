package edu.pcc.fbj.rankingsystem.resultreporting;

import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/**
 * @author David Li
 * @version 2016.05.19
 */
public class StatisticsReportDB  extends ReportDB implements ReportDAO
{
    /**
     * Constructor
     */
    public StatisticsReportDB()
    {

    }

    /**
     * Configure DAO with data from database, config files or anywhere else.
     */
    @Override
    public void Config()
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

        Object[][] statisticsResults = new Object[1][1];



        return statisticsResults;
    }

    /**
     * Retrieve column for report
     *
     * @param param HashMap<String, Object> param
     * @return List<String>
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
