package edu.pcc.fbj.rankingsystem.resultreporting;

import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author David Li
 * @version 2016.05.19
 */
public class BasicReportDB extends ReportDB implements ReportDAO
{
    /**
     * Basic report db constructor
     */
    public BasicReportDB()
    {

    }

    /**
     * Retrieve user test result according to user's email
     * @param   param HashMap<String, Object>
     * @return List<ReportTestResult>
     */
    @Override
    public Object[][] getUserTestResult(HashMap<String, Object> param)
    {
        List<ReportTestResult> results = new ArrayList<>();

        if(param == null)
        {
            ReportLogger.LOGGER.severe("Parameter is null.");
            return null;
        }

        setMessage(DATABASE_DATA_READING);
        try (
                PreparedStatement stmt = connection.prepareStatement(GET_USER_BASIC_REPORT_SQL)
        )
        {
            stmt.setString(1, (String)param.get("email"));
            stmt.setInt(2, Integer.parseInt((String)param.get("testID")));
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                results.add(new ReportTestResult(rs.getString("Name"),
                        rs.getInt("Wins"),
                        rs.getInt("Losses"),
                        rs.getInt("Ties"),
                        rs.getInt("Points")));
            }

            Object[][] userResults = new Object[results.size()][ReportTestResult.COLUMN_NUMBER];
            for (int i = 0; i < results.size(); i++)
            {
                userResults[i][0] = results.get(i).getItem1Name();
                userResults[i][1] = results.get(i).getWins();
                userResults[i][2] = results.get(i).getLosses();
                userResults[i][3] = results.get(i).getTies();
                userResults[i][4] = results.get(i).getScores();
            }
            setMessage(DATABASE_DATA_COMPLETE);
            stmt.close();
            return userResults;
        }
        catch (SQLException se)
        {
            ReportLogger.LOGGER.severe("Cannot get email list due to database exception.");
            return null;
        }
    }

    /**
     * retrieve column for report
     * @param param HashMap<String, Object> param
     * @return List<String>
     */
    @Override
    public List<String> getUserTestTableColumns(HashMap<String, Object> param)
    {
        List<String> basicTableColumn =  new ArrayList<>();
        basicTableColumn.add("Item");
        basicTableColumn.add("Wins");
        basicTableColumn.add("Losses");
        basicTableColumn.add("Ties");
        basicTableColumn.add("Score");

        return basicTableColumn;
    }

}
