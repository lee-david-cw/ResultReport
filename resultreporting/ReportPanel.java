package edu.pcc.fbj.rankingsystem.resultreporting;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.List;
import javax.swing.border.TitledBorder;

/**
 * Main GUI of reporting module
 *
 * @author  David Li
 * @version 2016.04.06
 *
 * @author David Li
 * @version 2016.04.21
 * 1. Center the column titles and values of the report table;
 * 2. Resize the size of the report table;
 *
 * @author David Li
 * @version 2016.04.24
 * 1. Display message on the panel when exception happens;
 */

public class ReportPanel extends JPanel implements ActionListener
{
    private JTable basicTable;
    private JTable matrixTable;
    private JTable statisticsTable;
    private JComboBox<String> userList;
    private JLabel label;
    private JComboBox<String> testNameList;
    private JComboBox<String> userTestIDList;
    private int emailIndex;
    private int testNameIndex;
    private int testIDIndex;
    private Signal signal;
    private JCheckBox basicReportButton;
    private JCheckBox matrixReportButton;
    private JCheckBox statisticsButton;
    private JScrollPane matrixPane;
    private JScrollPane statisticsPane;
    private boolean matrixReportFalg;
    private boolean statisticsReportFlag;
    private GridBagConstraints layout;
    JRadioButton firstChoiceBtn;
    JRadioButton XOverYBtn;

    /**
     * Default constructor
     */
    public ReportPanel()
    {
        emailIndex = 0;
        testNameIndex = 0;
        testIDIndex = 0;
        signal = Signal.DATABASE_SIGNAL_RETRIEVE_DATA;
        matrixReportFalg = true;
        statisticsReportFlag = true;
        initPanel();
    }

    /**
     * Initialize email combobox list
     * @param c GridBagConstraints
     */
    private void initWidgetEmailList(GridBagConstraints c)
    {
        label = new JLabel("Email List:");
        c.weightx = 0.5;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(10,20,0,10);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 3;
        c.ipadx = 200;
        add(label, c);

        userList = new JComboBox<String>();
        userList.setPreferredSize(new Dimension(300, 25));
        userList.addActionListener(this);
        userList.setActionCommand("EmailList");

        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(0,10,0,10);
        c.weightx = 0.5;
        c.gridx = 0;
        c.gridwidth = 3;
        c.gridy = 1;
        c.ipadx = 200;
        add(userList, c);
    }

    /**
     * Initialize Test Item list
     * @param c GridBagConstraints
     */
    private void initWidgetTestItemList(GridBagConstraints c)
    {
        JLabel testLabel = new JLabel("Test Item List:");
        c.weightx = 0.5;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(10,20,0,10);
        c.gridx = 0;
        c.gridy = 2;
        c.gridwidth = 3;
        c.ipadx = 200;
        add(testLabel, c);

        testNameList = new JComboBox<>();
        testNameList.setPreferredSize(new Dimension(300, 25));
        testNameList.addActionListener(this);
        testNameList.setActionCommand("TestNameList");

        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(0,10,0,10);
        c.weightx = 0.5;
        c.gridx = 0;
        c.gridwidth = 3;
        c.gridy = 3;
        add(testNameList, c);
    }

    /**
     * Initialzie user test id list
     * @param c GridBagConstraints
     */
    private void initWidgetUserTestList(GridBagConstraints c)
    {
        JLabel testLabel = new JLabel("User Test Id List:");
        c.weightx = 0.5;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(10,20,0,10);
        c.gridx = 0;
        c.gridy = 4;
        c.gridwidth = 3;
        c.ipadx = 200;
        add(testLabel, c);

        userTestIDList = new JComboBox<>();
        userTestIDList.setPreferredSize(new Dimension(300, 25));
        userTestIDList.addActionListener(this);
        userTestIDList.setActionCommand("TestIDList");

        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(0,10,0,10);
        c.weightx = 0.5;
        c.gridx = 0;
        c.gridwidth = 3;
        c.gridy = 5;
        add(userTestIDList, c);
    }

    /**
     * Initialize statistics option
     * @param c GridBagConstraints
     */
    private void initWidgetReportOption(GridBagConstraints c)
    {
        statisticsButton = new JCheckBox("Statistics");
        statisticsButton.setMnemonic(KeyEvent.VK_H);
        statisticsButton.setSelected(true);
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(10,20,1,10);
        c.weightx = 0.5;
        c.gridx = 0;
        c.gridwidth = 1;
        c.gridy = 6;
        statisticsButton.addActionListener(this);
        statisticsButton.setActionCommand("StatisticsReport");
        add(statisticsButton, c);

        firstChoiceBtn = new JRadioButton("Percentage of ranking each item first");
        firstChoiceBtn.setActionCommand("FirstChoice");
        firstChoiceBtn.setSelected(true);

        XOverYBtn = new JRadioButton("Percentage of ranking itemX over itemY");
        XOverYBtn.setActionCommand("XOverY");
        XOverYBtn.setSelected(true);

        ButtonGroup statisticsOption = new ButtonGroup();
        statisticsOption.add(firstChoiceBtn);
        statisticsOption.add(XOverYBtn);

        firstChoiceBtn.addActionListener(this);
        XOverYBtn.addActionListener(this);

        c.gridy = 7;
        add(firstChoiceBtn, c);
        c.gridy = 8;
        add(XOverYBtn, c);
    }

    /**
     * Initialize basic report table
     * @param c GridBagConstraints
     */
    private void initWidgetBasicReport(GridBagConstraints c)
    {

        if(basicTable == null)
        {
            basicTable = new JTable();
        }
        basicTable.setShowGrid(true);
        JScrollPane basicPane = new JScrollPane(basicTable);
        basicPane.setPreferredSize(new Dimension(400,300));
        basicPane.setBorder (BorderFactory.createTitledBorder (BorderFactory.createEtchedBorder (),
                "Basic Report",
                TitledBorder.CENTER,
                TitledBorder.TOP));

        c.fill = GridBagConstraints.HORIZONTAL;
        c.ipady = 1000;
        c.ipadx = 500;
        c.weighty = 1.0;
        c.insets = new Insets(0,10,10,10);
        c.gridx = 3;
        c.gridwidth = 3;
        c.gridheight = 10;
        c.gridy = 1;
        add(basicPane, c);
    }

    /**
     * initialize matrix report table
     * @param c GridBagConstraints
     */
    private void initWidgetMatrixReport(GridBagConstraints c)
    {
        matrixTable = new JTable();
        matrixTable.setShowGrid(true);
        matrixPane = new JScrollPane(matrixTable);
        matrixPane.setPreferredSize(new Dimension(400,300));
        matrixPane.setBorder (BorderFactory.createTitledBorder (BorderFactory.createEtchedBorder (),
                "Matrix Report",
                TitledBorder.CENTER,
                TitledBorder.TOP));

        c.fill = GridBagConstraints.HORIZONTAL;
        c.ipady = 1000;
        c.ipadx = 500;
        c.weighty = 1.0;
        c.insets = new Insets(0,10,10,10);
        c.gridx = 3;
        c.gridwidth = 3;
        c.gridy = 12;
        c.gridheight = 10;
        add(matrixPane, c);
        JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(this);
        if(frame != null)
        {
            frame.setSize(new Dimension(800, 600));
            frame.revalidate();
            frame.setLocationRelativeTo(null);
        }
    }

    /**
     * Initialize statistics report table
     * @param c GridBagConstraints
     */
    private void initWidgetStatisticsReport(GridBagConstraints c)
    {
        statisticsTable = new JTable();
        statisticsTable.setShowGrid(true);
        statisticsPane = new JScrollPane(statisticsTable);
        statisticsPane.setPreferredSize(new Dimension(400,300));
        statisticsPane.setBorder (BorderFactory.createTitledBorder (BorderFactory.createEtchedBorder (),
                "Statistics Report",
                TitledBorder.CENTER,
                TitledBorder.TOP));

        c.fill = GridBagConstraints.HORIZONTAL;
        c.ipady = 1000;
        c.ipadx = 500;
        c.weighty = 1.0;

        c.insets = new Insets(0,10,10,10);
        c.gridx = 0;
        c.gridwidth = 3;
        c.gridy = 12;
        c.gridheight = 10;
        add(statisticsPane, c);
        JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(this);
        if(frame != null)
        {
            frame.setSize(new Dimension(800, 600));
            frame.revalidate();
            frame.setLocationRelativeTo(null);
        }
    }


    /**
     * Initialize Report Panel
     */
    private void initPanel()
    {

        setLayout(new GridBagLayout());
        setPreferredSize(new Dimension(800, 600));
        layout = new GridBagConstraints();

        initWidgetEmailList(layout);
        initWidgetTestItemList(layout);
        initWidgetUserTestList(layout);
        initWidgetReportOption(layout);
        initWidgetBasicReport(layout);
        initWidgetMatrixReport(layout);
        initWidgetStatisticsReport(layout);
    }

    /**
     * Set label text
     * @param text String
     */
    public void setLabelText(String text)
    {
        label.setText(text);
    }

    /**
     * Set items for ComboBoxList
     * @param listItem Vector<String>
     */
    public void setEmailListItem(Vector<String> listItem)
    {
        if(listItem == null)
            return;

        for(String item: listItem)
        {
            this.userList.addItem(item);
        }
        userList.setSelectedIndex(0);
    }

    /**
     * Set items for user test id list ComboBoxList
     * @param listItem Vector<String>
     */
    public void setUserTestIDListItem(Vector<String> listItem)
    {
        if(listItem == null)
            return;

        this.userTestIDList.removeAllItems();
        for(String item: listItem)
        {
            this.userTestIDList.addItem(item);
        }
        userTestIDList.setSelectedIndex(0);

    }

    /**
     * Set items for user test name list
     * @param listItem Vector<String>
     */
    public void setUserTestNameListItem(Vector<String> listItem)
    {
        if(listItem == null)
            return;

        this.testNameList.removeAllItems();
        for(String item: listItem)
        {
            this.testNameList.addItem(item);
        }
        this.testNameList.setSelectedIndex(0);
    }

    /**
     * Fill basic report table
     * @param data Object[][]
     */
    public void setBasicReportData(Object[][] data)
    {
        if(data == null)
            return;

        DefaultTableModel model = (DefaultTableModel) basicTable.getModel();
        int rowCount = model.getRowCount();

        for(int i = rowCount-1; i>=0; i--)
        {
            model.removeRow(i);
        }

        model.setRowCount(data.length);
        for(int i = 0; i<data.length; i++)
        {
            for(int j=0; j<data[0].length; j++)
            {
                model.setValueAt(data[i][j], i, j);
            }
        }
    }

    /**
     * set basic report table columns
     * @param data List<String>
     */
    public void setBasicTableColumns(List<String> data)
    {
        DefaultTableModel model = (DefaultTableModel) basicTable.getModel();
        int rowCount = model.getRowCount();

        for(int i = rowCount-1; i>=0; i--)
        {
            model.removeRow(i);
        }

        model.setColumnCount(0);

        for(String name : data)
        {
            model.addColumn(name);
        }

        //Center table title and cell values
        ((DefaultTableCellRenderer)basicTable.getTableHeader().getDefaultRenderer()).setHorizontalAlignment(SwingConstants.CENTER);
        DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
        centerRenderer.setHorizontalAlignment( SwingConstants.CENTER );
        for(int i = 1;i < basicTable.getModel().getColumnCount(); i++)
        {
            basicTable.getColumnModel().getColumn(i).setCellRenderer(centerRenderer);
        }
    }

    /**
     * Set matrix table columns
     * @param data List<String>
     */
    public void setMatrixTableColumns(List<String> data) throws NoSuchElementException, ArrayIndexOutOfBoundsException
    {
        DefaultTableModel model = (DefaultTableModel) matrixTable.getModel();
        int rowCount = model.getRowCount();

        for(int i = rowCount-1; i>=0; i--)
        {
            model.removeRow(i);
        }

        model.setColumnCount(0);

        model.addColumn("");
        for(String name : data)
        {
            model.addColumn(name);
        }

        //Center table title and cell values
        ((DefaultTableCellRenderer)matrixTable.getTableHeader().getDefaultRenderer()).setHorizontalAlignment(SwingConstants.CENTER);
        DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
        centerRenderer.setHorizontalAlignment( SwingConstants.CENTER );
        for(int i = 1;i < matrixTable.getModel().getColumnCount(); i++)
        {
            matrixTable.getColumnModel().getColumn(i).setCellRenderer(centerRenderer);
        }


    }

    /**
     * Fill matrix report table
     * @param data Object[][]
     */
    public void setMatrixReportData(Object[][] data)
    {

        if(data == null)
        {
            return;
        }

        DefaultTableModel model = (DefaultTableModel) matrixTable.getModel();
        int rowCount = model.getRowCount();
        int columnCount = model.getColumnCount();

        for(int i = rowCount-1; i>=0; i--)
        {
            model.removeRow(i);
        }

        model.setRowCount(columnCount-1);
        rowCount = model.getRowCount();
        for(int i = 0; i<rowCount; i++)
        {
            model.setValueAt(model.getColumnName(i+1), i ,0);
            model.setValueAt("x", i ,i+1);
        }

        for(int i=0; i<data.length; i++)
        {
            int row = 0, column = 0;
            for(int j=1; j<columnCount; j++)
            {
                if(data[i][0].equals(model.getColumnName(j)))
                {
                    row = j;
                }
            }
                
            for(int k=1; k<columnCount; k++)
            {
                if(data[i][1].equals(model.getColumnName(k)))
                {
                    column = k;
                }
            }

            model.setValueAt((-1)* (int) data[i][2], row-1, column);
            model.setValueAt(data[i][2], column-1, row);
        }

    }

    /**
     * Fill statistics report table
     * @param data Object[][]
     */
    public void setStatisticsReportData(Object[][] data)
    {
        if(data == null)
            return;

        DefaultTableModel model = (DefaultTableModel) statisticsTable.getModel();
        int rowCount = model.getRowCount();

        for(int i = rowCount-1; i>=0; i--)
        {
            model.removeRow(i);
        }

        model.setRowCount(data.length);
        for(int i = 0; i<data.length; i++)
        {
            for(int j=0; j<data[0].length; j++)
            {
                model.setValueAt(data[i][j], i, j);
            }
        }

    }

    /**
     * set statistics report table columns
     * @param data List<String>
     */
    public void setStatisticsTableColumns(List<String> data)
    {
        if(data == null)
            return;

        DefaultTableModel model = (DefaultTableModel) statisticsTable.getModel();
        int rowCount = model.getRowCount();

        for(int i = rowCount-1; i>=0; i--)
        {
            model.removeRow(i);
        }

        model.setColumnCount(0);

        for(String name : data)
        {
            model.addColumn(name);
        }

        //Center table title and cell values
        ((DefaultTableCellRenderer)statisticsTable.getTableHeader().getDefaultRenderer()).setHorizontalAlignment(SwingConstants.CENTER);
        DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
        centerRenderer.setHorizontalAlignment( SwingConstants.CENTER );
        for(int i = 1;i < statisticsTable.getModel().getColumnCount(); i++)
        {
            statisticsTable.getColumnModel().getColumn(i).setCellRenderer(centerRenderer);
        }
    }

    /**
     * reset statistics radio to firstchoice
     */
    private void resetStatisticsOption()
    {
        if(statisticsButton.isSelected())
        {
            firstChoiceBtn.setSelected(true);
        }
    }

    /**
     * Action listener userd for respond to event of Changing item in ComboBoxList.
     * @param e  - action event
     */
    public void actionPerformed(ActionEvent e)
    {
        String command = e.getActionCommand();

        ReportLogger.LOGGER.info("Event -> " + command);

        if (command.equals("EmailList"))
        {
            JComboBox user = (JComboBox) e.getSource();
            setEmailIndex(user.getSelectedIndex());
            resetStatisticsOption();
            setSignal(Signal.DATABASE_SIGNAL_UPDATE_EAMIL_LIST);
        }
        else if (command.equals("TestNameList"))
        {
            JComboBox testName = (JComboBox) e.getSource();
            setTestNameIndex(testName.getSelectedIndex());
            resetStatisticsOption();
            setSignal(Signal.DATABASE_SIGNAL_UPDATE_TESTNAME_LIST);
        }
        else if (command.equals("TestIDList"))
        {
            JComboBox testID = (JComboBox) e.getSource();
            setTestIDIndex(testID.getSelectedIndex());
            resetStatisticsOption();
            setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_DATA);
        }
        else if (command.equals("MatrixReport"))
        {
            if(matrixReportButton.isSelected()) {
                GridBagConstraints c = new GridBagConstraints();
                initWidgetMatrixReport(c);
                enableMatrixReport();
                setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_DATA);
            }
            else
            {
                remove(matrixPane);
                disableMatrixReport();
                setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_DATA);
                JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(this);
                if(frame != null)
                {
                    if(!isStatisticsReportEnabled())
                    {
                        frame.setSize(new Dimension(800, 300));
                    }
                    frame.revalidate();
                    frame.getContentPane().validate();
                    frame.getContentPane().repaint();
                    frame.setLocationRelativeTo(null);
                }
            }
        }
        else if (command.equals("StatisticsReport"))
        {
            if(statisticsButton.isSelected())
            {
                System.out.println(command);
                GridBagConstraints c = new GridBagConstraints();
                initWidgetStatisticsReport(c);
                enableStatisticsReport();
                firstChoiceBtn.setEnabled(true);
                XOverYBtn.setEnabled(true);
                firstChoiceBtn.setSelected(true);
                setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_STATISTICS_FIRSTCHOICE_DATA);
            }
            else
            {
                remove(statisticsPane);
                firstChoiceBtn.setEnabled(false);
                XOverYBtn.setEnabled(false);
                disableStatisticsReport();
                JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(this);
                if(frame != null)
                {
                    if(!isMatrixReportEnabled())
                    {
                        frame.setSize(new Dimension(800, 300));
                    }
                    frame.revalidate();
                    frame.getContentPane().validate();
                    frame.getContentPane().repaint();
                    frame.setLocationRelativeTo(null);
                }
            }
        }
        else if(command.equals("FirstChoice"))
        {
            setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_STATISTICS_FIRSTCHOICE_DATA);
        }
        else if(command.equals("XOverY"))
        {
            setSignal(Signal.DATABASE_SIGNAL_RETRIEVE_STATISTICS_XOVERY_DATA);
        }

    }


    /**
     * Get email index
     * @return int
     */
    public int getEmailIndex()
    {
        return emailIndex;
    }

    /**
     * Get Test Name List Index
     * @return int
     */
    public int getTestNameIndex()
    {
        return testNameIndex;
    }

    /**
     * Get Test ID List Index
     * @return int
     */
    public int getTestIDIndex()
    {
        return testIDIndex;
    }

    /**
     * Set email index
     */
    private void setEmailIndex(int index)
    {
        emailIndex = index;
    }

    /**
     * Set Test Name List Index
     * @param index int
     */
    private void setTestNameIndex(int index)
    {
        testNameIndex = index;
    }

    /**
     * Set Test Name List Index
     * @param index int
     */
    private void setTestIDIndex(int index)
    {
        testIDIndex = index;
    }

    /**
     * Get signal
     * @return Signal
     */
    public Signal getSignal()
    {
        SignalLog();
        return signal;
    }

    /**
     * Logging signal
     */
    private void SignalLog()
    {
        switch(signal)
        {
            case DATABASE_SIGNAL_THREAD_WAIT:
                ReportLogger.LOGGER.fine("Signal to waiting for thread.");
                break;
            case DATABASE_SIGNAL_THREAD_TERMINATE:
                ReportLogger.LOGGER.info("Signal to terminate thread.");
                break;
            case DATABASE_SIGNAL_UPDATE_EAMIL_LIST:
                ReportLogger.LOGGER.info("Signal to get email list.");
                break;
            case DATABASE_SIGNAL_UPDATE_TESTNAME_LIST:
                ReportLogger.LOGGER.info("Signal to get testname list.");
                break;
            case DATABASE_SIGNAL_UPDATE_TESTID_LIST:
                ReportLogger.LOGGER.info("Signal to get testid list");
                break;
            case DATABASE_SIGNAL_RETRIEVE_DATA:
                ReportLogger.LOGGER.info("Signal to retrieve test data.");
                break;
            case DATABASE_SIGNAL_RETRIEVE_MATRIX_DATA:
                ReportLogger.LOGGER.info("Signal to get matrix report.");
                break;
            case DATABASE_SIGNAL_RETRIEVE_STATISTICS_FIRSTCHOICE_DATA:
                ReportLogger.LOGGER.info("Signal to get statistics firstchoice report.");
                break;
            case DATABASE_SIGNAL_RETRIEVE_STATISTICS_XOVERY_DATA:
                ReportLogger.LOGGER.info("Signal to get statistics itemX Over itemY report.");
                break;
        }
    }

    /**
     * Set signal
     */
    public void setSignal(Signal signal)
    {
        this.signal = signal;
    }

    /**
     * enable matrix report
     */
    private void enableMatrixReport()
    {
        matrixReportFalg = true;
    }

    /**
     * disable matrix report
     */
    private void disableMatrixReport()
    {
        matrixReportFalg = false;
    }

    /**
     * is matrix report enabled?
     * @return boolean
     */
    public boolean isMatrixReportEnabled()
    {
        return matrixReportFalg;
    }

    /**
     * enable statistics report
     */
    private void enableStatisticsReport()
    {
        statisticsReportFlag = true;
    }

    /**
     * disable statistics report
     */
    private void disableStatisticsReport()
    {
        statisticsReportFlag = false;
    }

    /**
     * Is statistics report enabled?
     * @return boolean
     */
    public boolean isStatisticsReportEnabled()
    {
        return statisticsReportFlag;
    }

    /**
     * Display debug information Used for debugging.
     * @param  table JTable
     */
    private void printDebugData(JTable table)
    {
        int numRows = table.getRowCount();
        int numCols = table.getColumnCount();
        javax.swing.table.TableModel model = table.getModel();

        ReportLogger.LOGGER.info("Value of data: ");
        for (int i=0;i < numRows; i++)
        {
            ReportLogger.LOGGER.info("    row " + i + ":");
            for (int j=0; j < numCols; j++)
            {
                ReportLogger.LOGGER.info("  " + model.getValueAt(i, j));
            }
            System.out.println();
        }
    }

}
