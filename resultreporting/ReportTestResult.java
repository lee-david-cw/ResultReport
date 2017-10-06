package edu.pcc.fbj.rankingsystem.resultreporting;

/**
 * Test Result Class defining test report structure
 *
 * @author David Li
 * @version 2016.04.14
 */
public class ReportTestResult
{

    private String item1Name;
    private String item2Name;
    private int value;
    private int wins;
    private int losses;
    private int ties;
    private int scores;
    static final int COLUMN_NUMBER = 5;

    /**
     * Constructor
     * @param item1Name - item name
     * @param wins - points of wins
     * @param losses - points of losses
     * @param ties - points of ties
     */
    public ReportTestResult(String item1Name, int wins, int losses, int ties, int scores)
    {

        this.item1Name = item1Name;
        this.wins = wins;
        this.losses = losses;
        this.ties = ties;
        this.scores = scores;
    }

    /**
     * Constructor
     * @param item1 - points of wins
     * @param item2 - points of losses
     * @param value - points of ties
     */
    public ReportTestResult(String item1, String item2, int value)
    {

        this.item1Name = item1;
        this.item2Name = item2;
        this.value = value;
    }


    /**
     * Retrieve item1 name
     * @return String item1Name
     */
    public String getItem1Name ()
    {
        return item1Name;
    }

    /**
     * Retrieve item2 name
     * @return String item2Name
     */
    public String getItem2Name ()
    {
        return item2Name;
    }

    /**
     * Retrieve paire value
     * @return int value
     */
    public int getPairValue ()
    {
        return value;
    }

    /**
     * Retrieve points of wins
     * @return  int wins
     */
    public int getWins () {
        return wins;
    }

    /**
     * Retrieve points of losses
     * @return int losses
     */
    public int getLosses() {
        return losses;
    }

    /**
     * Retrieve points of ties
     * @return int ties
     */
    public int getTies () {
        return ties;
    }

    /**
     * Retrieve total score;
     */
    public int getScores () {
        return scores;
    }

    /**
     * Print data
     * @return String
     */
    @Override
    public String toString()
    {
        return "User Test Result-> "
                + " Item 1: " + item1Name
                + " Wins: " + wins
                + " Losses: " + losses
                + " Ties: " + ties
                + " Score: " + scores;
    }
}
