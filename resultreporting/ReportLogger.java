package edu.pcc.fbj.rankingsystem.resultreporting;

import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * @author David Li
 * @version 2016.05.30
 */
public class ReportLogger
{
    public static final Logger LOGGER = Logger.getLogger(ReportLogger.class.getName());


    /**
     * Set log level
     * @param level String
     */
    public static void setLevel(String level)
    {
        switch (level)
        {
            case "ALL":
                LOGGER.setLevel(Level.ALL);
                break;
            case "INFO":
                LOGGER.setLevel(Level.INFO);
                break;
            case "WARNING":
                LOGGER.setLevel(Level.WARNING);
                break;
            case "SEVERE":
                LOGGER.setLevel(Level.SEVERE);
                break;
            case "OFF":
                LOGGER.setLevel(Level.OFF);
                break;
        }
    }

}
