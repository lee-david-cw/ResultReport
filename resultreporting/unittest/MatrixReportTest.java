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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * @author David Li
 * @version 2016.05.31
 */
@RunWith(Parameterized.class)
public class MatrixReportTest
{
    ReportDAO matrixReport;
    HashMap<String, Object> input;

    @Before
    public void initialize()
    {
        matrixReport = ReportDAOFactory.getMatrixReportDAO();
    }

    public MatrixReportTest(HashMap<String, Object> input)
    {
        this.input = input;
    }

    @Parameterized.Parameters
    public static Collection inputParameter()
    {
        HashMap<String, Object> badTestID = new HashMap<>();
        badTestID.put("email", "TestResult@yahoo.com");
        badTestID.put("testID", "10");

        HashMap<String, Object> badEmail = new HashMap<>();
        badEmail.put("email", "wrong@yahoo.com");
        badEmail.put("testID", "10");

        HashMap<String, Object>  goodInput = new HashMap<>();
        goodInput.put("email", "testing1@yahoo.com");
        goodInput.put("testID", "159");

        return Arrays.asList(
                badEmail,
                badTestID,
                goodInput,
                null
        );
    }

    @Test
    public void testInitMatrixReport()
    {
        assertNotNull(matrixReport);
    }

    @Test
    public void testDBConnection()
    {
        assertNotNull(matrixReport.DBConnection());
    }

    @Test
    public void testGetColumnOfMatrixReport()
    {
        List<String> column;
        matrixReport.DBConnection();
        column = matrixReport.getUserTestTableColumns(input);
        assertNotNull(column);
        for(String col: column)
        {
            ReportLogger.LOGGER.info(col);
        }
    }

    @Test
    public void testGetdataOfMatrixReport()
    {
        Object[][] results;

        matrixReport.DBConnection();
        ReportLogger.LOGGER.info((String)input.get("email"));
        ReportLogger.LOGGER.info((String)input.get("testID"));
        results = matrixReport.getUserTestResult(input);
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
