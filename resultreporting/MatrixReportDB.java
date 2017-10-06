package edu.pcc.fbj.rankingsystem.resultreporting;

import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author David li
 * @version 2016.05.19
 */
public class MatrixReportDB extends ReportDB implements ReportDAO
{
    /**
     * Matrix report db constructor
     */
    public MatrixReportDB()
    {
    }

    /**
     * Retrieve test result
     *
     * @param param HashMap<String, Object>
     * @return Object[][]
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
                PreparedStatement stmt = connection.prepareStatement(GET_USER_MATRIX_REPORT_SQL)
        )
        {
            stmt.setString(1, (String)param.get("email"));
            stmt.setInt(2, Integer.parseInt((String)param.get("testID")));
            ResultSet rs = stmt.executeQuery();
            while (rs.next())
            {
                results.add(new ReportTestResult(
                        rs.getString("Item1"),
                        rs.getString("Item2"),
                        rs.getInt("Value")));
            }

            Object[][] userResults = new Object[results.size()][ReportTestResult.COLUMN_NUMBER];
            for (int i = 0; i < results.size(); i++)
            {
                userResults[i][0] = results.get(i).getItem1Name();
                userResults[i][1] = results.get(i).getItem2Name();
                userResults[i][2] = results.get(i).getPairValue();
            }
            setMessage(DATABASE_DATA_COMPLETE);
            stmt.close();
            return userResults;
        }
        catch (SQLException se)
        {
            ReportLogger.LOGGER.severe("Cannot get test result due to database exception.");
            return null;
        }
    }

    /**
     * Retrieve column title for report
     *
     * @param param HashMap<String, Object>
     * @return  List<String>
     */
    @Override
    public List<String> getUserTestTableColumns(HashMap<String, Object> param)
    {
        List<String> results = new ArrayList<>();

        if(param == null)
        {
            ReportLogger.LOGGER.severe("Parameter is null.");
            return null;
        }

        setMessage(DATABASE_DATA_READING);
        try (
                PreparedStatement stmt = connection.prepareStatement(GET_USER_MATRIX_ITEMS_SQL)
        )
        {
            stmt.setString(1, (String)param.get("email"));
            stmt.setInt(2, Integer.parseInt((String)param.get("testID")));
            ResultSet rs = stmt.executeQuery();
            while (rs.next())
            {
                results.add(rs.getString("Item1"));
            }

            setMessage(DATABASE_DATA_COMPLETE);
            stmt.close();
            return results;
        }
        catch (SQLException se)
        {
            ReportLogger.LOGGER.severe("Cannot get columns due to database exception.");
            return null;
        }
    }

}
