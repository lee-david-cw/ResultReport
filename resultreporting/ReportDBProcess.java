package edu.pcc.fbj.rankingsystem.resultreporting;

import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;

import java.util.*;

/**
 * Start a thread to process database transaction
 * @author David Li
 * @version 2016.04.28
 */
public class ReportDBProcess implements Runnable
{
    private HashMap<String, ReportDAO> dao;
    private ReportPanel reportPane;
    private Signal signal;

    /**
     * Constructor
     * @param dao ReportDAO
     * @param reportPane ReportPanel
     */
    public ReportDBProcess(HashMap<String, ReportDAO> dao, ReportPanel reportPane)
    {
        this.dao = dao;
        this.reportPane = reportPane;
    }

    /**
     * Start DB Processing thread
     */
    public void run()
    {
        ReportLogger.LOGGER.info("Start a DB processing thread");

        Vector<String> userEmailList = null;
        Vector<String> userTestNameList = null;
        Vector<String> userTestIDList = null;
        HashMap<String, Object>  elementForReport = new HashMap<>();

        if(dao.get("Basic").DBConnection() != null)
        {
            userEmailList = dao.get("Basic").getUserEmailList();
            if(userEmailList != null)
            {
                reportPane.setEmailListItem(userEmailList);
                userTestNameList = dao.get("Basic").getUserTestNameList(userEmailList.get(reportPane.getEmailIndex()));
                if(userTestNameList != null && userTestNameList.size() > 0)
                {
                    reportPane.setUserTestNameListItem(userTestNameList);
                    userTestIDList = dao.get("Basic").getUserTestID(userEmailList.get(reportPane.getEmailIndex()), userTestNameList.get(reportPane.getTestNameIndex()));
                    reportPane.setUserTestIDListItem(userTestIDList);
                }
                reportPane.setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_DATA);
            }
        }

        while(true)
        {
            if(reportPane.getSignal() == signal.DATABASE_SIGNAL_UPDATE_EAMIL_LIST && userEmailList != null)
            {
                userTestNameList = dao.get("Basic").getUserTestNameList(userEmailList.get(reportPane.getEmailIndex()));
                if(userTestNameList != null && userTestNameList.size() > 0 )
                {
                    reportPane.setUserTestNameListItem(userTestNameList);
                    userTestIDList = dao.get("Basic").getUserTestID(userEmailList.get(reportPane.getEmailIndex()), userTestNameList.get(reportPane.getTestNameIndex()));
                    reportPane.setUserTestIDListItem(userTestIDList);
                }
                reportPane.setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_DATA);
            }

            if(reportPane.getSignal() == signal.DATABASE_SIGNAL_UPDATE_TESTNAME_LIST && userEmailList != null && userTestNameList != null && userTestNameList.size() > 0 )
            {
                userTestIDList = dao.get("Basic").getUserTestID(userEmailList.get(reportPane.getEmailIndex()), userTestNameList.get(reportPane.getTestNameIndex()));
                reportPane.setUserTestIDListItem(userTestIDList);
                reportPane.setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_DATA);
            }

            if(reportPane.getSignal() == signal.DATABASE_SIGNAL_RETRIEVE_DATA )
            {

                if(dao.get("Basic").DBConnection() != null && userEmailList != null && userTestIDList != null)
                {

                    reportPane.setBasicTableColumns(dao.get("Basic").getUserTestTableColumns(null));
                    elementForReport.put("email", userEmailList.get(reportPane.getEmailIndex()));
                    elementForReport.put("testID", userTestIDList.get(reportPane.getTestIDIndex()));
                    reportPane.setBasicReportData(dao.get("Basic").getUserTestResult(elementForReport));
                    elementForReport.clear();
                    dao.get("Basic").setMessage(dao.get("Basic").DATABASE_DATA_SELECTION);

                    if(reportPane.isMatrixReportEnabled())
                    {
                        if(dao.get("Matrix").DBConnection() != null && userEmailList != null & userTestIDList != null)
                        {
                            elementForReport.put("email", userEmailList.get(reportPane.getEmailIndex()));
                            elementForReport.put("testID", userTestIDList.get(reportPane.getTestIDIndex()));
                            reportPane.setMatrixTableColumns(dao.get("Matrix").getUserTestTableColumns(elementForReport));
                            reportPane.setMatrixReportData(dao.get("Matrix").getUserTestResult(elementForReport));
                            elementForReport.clear();
                            dao.get("Matrix").setMessage(dao.get("Matrix").DATABASE_DATA_SELECTION);
                        }
                    }

                    if(reportPane.isStatisticsReportEnabled())
                    {
                        if(dao.get("Statistics.FirstChoice").DBConnection() != null && userTestNameList != null && userTestNameList.size() > 0)
                        {
                            elementForReport.put("TestName", userTestNameList.get(reportPane.getTestNameIndex()));
                            reportPane.setStatisticsTableColumns(dao.get("Statistics.FirstChoice").getUserTestTableColumns(null));
                            reportPane.setStatisticsReportData(dao.get("Statistics.FirstChoice").getUserTestResult(elementForReport));
                            elementForReport.clear();
                            dao.get("Statistics.FirstChoice").setMessage(dao.get("Statistics.FirstChoice").DATABASE_DATA_SELECTION);
                        }
                    }

                    reportPane.setSignal(signal.DATABASE_SIGNAL_THREAD_WAIT);
                }

            }
            else if(reportPane.getSignal() == signal.DATABASE_SIGNAL_RETRIEVE_STATISTICS_FIRSTCHOICE_DATA)
            {
                if(reportPane.isStatisticsReportEnabled())
                {
                    if(dao.get("Statistics.FirstChoice").DBConnection() != null && userTestNameList != null && userTestNameList.size() > 0)
                    {
                        elementForReport.put("TestName", userTestNameList.get(reportPane.getTestNameIndex()));
                        reportPane.setStatisticsTableColumns(dao.get("Statistics.FirstChoice").getUserTestTableColumns(null));
                        reportPane.setStatisticsReportData(dao.get("Statistics.FirstChoice").getUserTestResult(elementForReport));
                        elementForReport.clear();
                        dao.get("Statistics.FirstChoice").setMessage(dao.get("Statistics.FirstChoice").DATABASE_DATA_SELECTION);
                    }
                    reportPane.setSignal(signal.DATABASE_SIGNAL_THREAD_WAIT);
                }
            }
            else if(reportPane.getSignal() == signal.DATABASE_SIGNAL_RETRIEVE_STATISTICS_XOVERY_DATA)
            {
                if(reportPane.isStatisticsReportEnabled())
                {
                    if(dao.get("Statistics.XOverY").DBConnection() != null && userTestNameList != null && userTestNameList.size() > 0)
                    {
                        elementForReport.put("TestName", userTestNameList.get(reportPane.getTestNameIndex()));
                        reportPane.setStatisticsTableColumns(dao.get("Statistics.XOverY").getUserTestTableColumns(null));
                        reportPane.setStatisticsReportData(dao.get("Statistics.XOverY").getUserTestResult(elementForReport));
                        elementForReport.clear();
                        dao.get("Statistics.XOverY").setMessage(dao.get("Statistics.XOverY").DATABASE_DATA_SELECTION);
                    }
                }

                reportPane.setSignal(signal.DATABASE_SIGNAL_THREAD_WAIT);
            }
            else if(reportPane.getSignal() == signal.DATABASE_SIGNAL_THREAD_TERMINATE)
            {
                ReportLogger.LOGGER.info("End a DB processing thread");
                break;
            }

            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                ReportLogger.LOGGER.severe("Thread is interrupted!");
            }
        }
    }
}
