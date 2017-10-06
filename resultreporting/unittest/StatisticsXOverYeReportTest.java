package edu.pcc.fbj.rankingsystem.resultreporting.unittest;

import edu.pcc.fbj.rankingsystem.resultreporting.ReportLogger;
import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;
import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAOFactory;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * @author David Li
 * @version 2016.05.31
 */
@RunWith(Parameterized.class)
public class StatisticsXOverYeReportTest
{
    ReportDAO xOverYReport;
    HashMap<String, Object> input;

    @Before
    public void initialize()
    {
        xOverYReport = ReportDAOFactory.getBasicReportDAO();
    }

    public StatisticsXOverYeReportTest(HashMap<String, Object> input)
    {
        this.input = input;
    }

    @Parameterized.Parameters
    public static Collection inputParameter()
    {
        HashMap<String, Object> goodInput = new HashMap<>();
        goodInput.put("TestName", "Animal Test");

        HashMap<String, Object> wrongInput = new HashMap<>();
        wrongInput.put("TestName", "wrong");

        HashMap<String, Object>  nullStringInput = new HashMap<>();
        nullStringInput.put("TestName", "");

        return Arrays.asList(
                goodInput,
                wrongInput,
                nullStringInput,
                null
        );
    }

    @Test
    public void testInitBasicReport()
    {
        assertNotNull(xOverYReport);
    }

    @Test
    public void testDBConnection()
    {
        assertNotNull(xOverYReport.DBConnection());
    }

    @Test
    public void testGetColumnOfBasicReport()
    {
        List<String> column;
        column = xOverYReport.getUserTestTableColumns(null);
        assertNotNull(column);
        for(String col: column)
        {
            ReportLogger.LOGGER.info(col);
        }
    }

    @Test
    public void testGetdataOfBasicReport()
    {
        Object[][] results;

        xOverYReport.DBConnection();
        ReportLogger.LOGGER.info((String)input.get("TestName"));
        results = xOverYReport.getUserTestResult(input);
        assertNotNull(results);
        for (int i = 0; i < results.length; i++)
        {
            String result="";
            for(int j = 0; j< results[0].length; j++)
            {
                result = result + results[i][j].toString() + " ";
            }
            ReportLogger.LOGGER.info(result);
        }
    }
}
