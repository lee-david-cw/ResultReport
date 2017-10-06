package edu.pcc.fbj.rankingsystem.resultreporting.unittest;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;


/**
 * @author David Li
 * @version 2016.05.31
 */

public class ReportUnitTestMain
{
    public static void main(String[] args)
    {
        Result result = JUnitCore.runClasses(JunitTestSuite.class);
        for (Failure failure : result.getFailures())
        {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}