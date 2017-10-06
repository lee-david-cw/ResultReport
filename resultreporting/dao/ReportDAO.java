package edu.pcc.fbj.rankingsystem.resultreporting.dao;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/**
 * ReportDAO is used for defining methods to retrieve report data.
 *
 * @author David Li
 * @version 2016.04.14
 */
public interface ReportDAO
{
    String DATABASE_MESSAGE_INIT = "Database initail ...";
    String DATABASE_CONNECTION_CONNECTING = "Connecting ...";
    String DATABASE_CONNECTION_FAILED = "Failed to connect!";
    String DATABASE_CONNECTION_SUCCESS = "Connect successfully!";
    String DATABASE_DATA_READING = "Retrieving data ...";
    String DATABASE_DATA_COMPLETE = "Reading data complete!";
    String DATABASE_DATA_SELECTION = "Email List:";

    String GET_USER_EMAIL_LIST_SQL
            = "SELECT DISTINCT FBJ_USER.Email FROM FBJ_USER " +
            " JOIN FBJ_TEST ON FBJ_USER.PK_UserID = FBJ_TEST.FK_UserID";

    String GET_USER_TEST_NAME_LIST_SQL
            = "SELECT Name FROM FBJ_TEST_NAME_LIST" +
            " WHERE Email = ?";

    String GET_USER_TEST_ID_LIST_SQL
            = "SELECT TestID FROM FBJ_USER_TEST_ID_LIST " +
            " WHERE UserEmail = ? AND TestName = ? ORDER BY TestID DESC";

    String GET_USER_BASIC_REPORT_SQL =
            " SELECT Item1 AS Name, Wins, Losses, Ties, Points FROM FBJ_BASIC_REPORT" +
                    " WHERE Email = ? AND FK_TestID = ?" +
                    " ORDER BY Points DESC";

    String GET_USER_MATRIX_REPORT_SQL =
            "SELECT Item1, Item2, Value FROM FBJ_MATRIX_REPORT" +
                    " WHERE Email = ? AND FK_TestID = ?";

    String GET_USER_MATRIX_ITEMS_SQL =
            "SELECT Item1 FROM FBJ_MATRIX_ITEMS WHERE Email = ? AND FK_TestID = ?";


    Vector<String> getUserEmailList();
    Vector<String> getUserTestID(String email, String testName);
    Vector<String> getUserTestNameList(String email);
    Object[][] getUserTestResult(HashMap<String, Object> param);
    List<String> getUserTestTableColumns(HashMap<String, Object> param);
    Connection DBConnection();
    String getMessage();
    void setMessage(String msg);
    void Config();
}
