package edu.pcc.fbj.rankingsystem.resultreporting.unittest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author David Li
 * @version 2015.05.30
 */
//JUnit Suite Test
@RunWith(Suite.class)
@Suite.SuiteClasses({
        BasicReportTest.class,
        MatrixReportTest.class,
        StatisticsFristChoiceReportTest.class,
        StatisticsXOverYeReportTest.class
})
public class JunitTestSuite
{

}
