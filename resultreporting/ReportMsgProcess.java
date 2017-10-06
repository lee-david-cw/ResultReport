package edu.pcc.fbj.rankingsystem.resultreporting;

import edu.pcc.fbj.rankingsystem.resultreporting.dao.ReportDAO;

import java.util.HashMap;
import java.util.List;

/**
 * Start a Message thread to display message while reading data from database.
 *
 * @author David Li
 * @version 2016.04.28
 */
public class ReportMsgProcess implements Runnable
{
    private HashMap<String, ReportDAO> dao;
    private ReportPanel reportPane;

    /**
     * Constructor
     * @param dao ReportDAO
     * @param reportPane ReportPanel
     */
    public ReportMsgProcess(HashMap<String, ReportDAO> dao, ReportPanel reportPane)
    {
        this.dao = dao;
        this.reportPane = reportPane;
    }

    /**
     * Start a message thread
     */
    public void run()
    {
        ReportLogger.LOGGER.info("Start a message processing thread");
        while(reportPane.getSignal() != Signal.DATABASE_SIGNAL_THREAD_TERMINATE)
        {
            reportPane.setLabelText(dao.get("Basic").getMessage());
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        reportPane.setLabelText(dao.get("Basic").getMessage());
        ReportLogger.LOGGER.info("End a message processing thread");
    }
}