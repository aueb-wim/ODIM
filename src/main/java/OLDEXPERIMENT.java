import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import it.unibas.spicy.model.algebra.query.operators.sql.GenerateSQL;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.persistence.relational.DAORelational;
import it.unibas.spicy.persistence.relational.DBFragmentDescription;
import it.unibas.spicy.persistence.relational.IConnectionFactory;
import it.unibas.spicy.persistence.relational.SimpleDbConnectionFactory;
import it.unibas.spicy.utility.SpicyEngineConstants;
import mipmapreduced.DbConnector;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OLDEXPERIMENT {

    private final static String EXP_1 = "C:" + File.separator +"Users" + File.separator + "Artem" + File.separator + "OneDrive" + File.separator +
            "Desktop" + File.separator + "MIPMAP" +File.separator  + "EXPERIMENTS" +File.separator + "remappingtasks" +File.separator + "freiburg_map_Artemis";
    private final static String dbResources = "C:\\Users\\Artem\\OneDrive\\Desktop\\IVM-NEW\\src\\main\\resources\\postgresdb.properties";
    private static final short EXP_INDEX = 2;


    public static void main(String[] args) {
        new DbConnector().configureDatabaseProperties(dbResources);
        setup();
        for (int j = 1; j < (int) Math.pow(2, EXP_INDEX)  + 1; j++) {
            MappingTask task = obtainUnionTask(EXP_1, (int) Math.pow(2, EXP_INDEX));
            final String sqlQuery = new GenerateSQL().generateSQL(task, 1);
            //System.out.println(sqlQuery);//.replaceAll("drop table if exists source1(.*\\s*.*\\s*.*\\s*.*)","");//extend it!
            try {
                final long start = System.nanoTime();
                //new DAOCsv().loadInstance(1, task.getSourceProxy(), true);
                loadInstance(task.getSourceProxy()); //ppmi only
                runQuery(sqlQuery, IVMUtility.obtainAccessConfiguration());
                if (j <(int) Math.pow(2, EXP_INDEX)) {
                    unifySourceData(EXP_1, (int) Math.pow(2, EXP_INDEX), j);
                }

                System.out.println( (double) (System.nanoTime() - start) / 1000000000);
                try {
                    if (j < (int) Math.pow(2, EXP_INDEX)) {
                        runQuery("DROP SCHEMA source1 CASCADE;", IVMUtility.obtainAccessConfiguration());
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println("boom");
                }
            } catch (DAOException e) {
                e.printStackTrace();
                System.out.println("boom");
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("boom");
            }

        }


    }

    private static void loadInstance(final IDataSourceProxy dataSource) throws SQLException, DAOException {
        IConnectionFactory connectionFactory = null;
        Connection connection = null;
        AccessConfiguration accessConfiguration = new AccessConfiguration();
        accessConfiguration.setDriver(SpicyEngineConstants.ACCESS_CONFIGURATION_DRIVER);
        accessConfiguration.setUri(SpicyEngineConstants.ACCESS_CONFIGURATION_URI + SpicyEngineConstants.MAPPING_TASK_DB_NAME);
        accessConfiguration.setLogin(SpicyEngineConstants.ACCESS_CONFIGURATION_LOGIN);
        accessConfiguration.setPassword(SpicyEngineConstants.ACCESS_CONFIGURATION_PASS);
        try {
            connectionFactory = new SimpleDbConnectionFactory();
            connection = connectionFactory.getConnection(accessConfiguration);
            Statement statement = connection.createStatement();

            HashMap<String, ArrayList<Object>> strfullPath = (HashMap<String, ArrayList<Object>>) dataSource.getAnnotation(SpicyEngineConstants.INSTANCE_PATH_LIST);

            for (Map.Entry<String, ArrayList<Object>> entry : strfullPath.entrySet()) {
                String filePath = entry.getKey();
                //the list entry.getValue() contains a)the table name
                //b)a boolean that contains the info if the instance file includes column names
                //and c) a boolean that contains the info if the instance file has been already loaded
                boolean loaded = (Boolean) entry.getValue().get(2);
                if (!loaded) {
                    String tableName = (String) entry.getValue().get(0);
                    tableName = SpicyEngineConstants.SOURCE_SCHEMA_NAME + 1 + ".\"" + tableName + "\"";
                    boolean colNames = (Boolean) entry.getValue().get(1);

                    //avenet
                    //CSVReader reader = new CSVReader(new FileReader(filePath));
                   // System.out.println(Paths.get(filePath.replace("\\", "/").replaceFirst("/","")));
                    Reader r = Files.newBufferedReader(Paths.get(filePath.replace("\\", "/").replaceFirst("/C:","")));
                    //CSVReader reader = new CSVReaderBuilder(r).withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS).build();
                    CSVParser csvParser;
                    if (colNames) {
                         csvParser = new CSVParser(r, CSVFormat.DEFAULT.withHeader());
                    }
                    else {
                        csvParser = new CSVParser(r, CSVFormat.DEFAULT);
                    }
                    try {
                        //ignore the first line if file includes column names

                        String[] nextLine;
                        String values;

                        ArrayList<String> stmnt_list = new ArrayList<String>();
                        String sql_insert_stmnt = "";
                        int line = 0;
                        //while ((nextLine = reader.readNext()) != null) {//for each line in the file
                        for (CSVRecord record: csvParser) {
                            line++;
                            //skip empty lines at the end of the csv file
                            if (record.size() != 1 || !record.get(0).isEmpty()) {
                                //insert into batches (of 500 rows)
                                if (line % 500 == 0) {
                                    //take out the last ',' character
                                    sql_insert_stmnt = sql_insert_stmnt.substring(0, sql_insert_stmnt.length() - 1);
                                    stmnt_list.add(sql_insert_stmnt);
                                    sql_insert_stmnt = "";
                                }
                                values = "";
                                for (int i = 0; i < record.size(); i++) {
                                    //avenet 20/7
                                    if (record.get(i) != null && !record.get(i).isEmpty()) {
//                                    if (!nextLine[i].equalsIgnoreCase("null")){
//                                        //replace double quotes with single quotes
//                                        //while first escape the character ' for SQL (the "replaceAll" method call)
                                        values += "'" + record.get(i).trim().replaceAll("'", "''") + "',";
//                                    }
//                                    //do not put quotes if value is the string null
//                                    else{
//                                        values += nextLine[i].trim().replaceAll("'", "''") + ",";
//                                    }
                                    } else {
                                        values += "null,";
                                    }
                                }
                                //take out the last ',' character
                                values = values.substring(0, values.length() - 1);
                                sql_insert_stmnt += "(" + values + "),";
                            }
                        }
                        csvParser.close();
                        if (sql_insert_stmnt != "") {
                            //take out the last ',' character
                            sql_insert_stmnt = sql_insert_stmnt.substring(0, sql_insert_stmnt.length() - 1);
                            stmnt_list.add(sql_insert_stmnt);
                            for (String stmnmt : stmnt_list) {
                                statement.executeUpdate("insert into " + tableName + " values " + stmnmt + ";");
                            }
                        }

                        //change the "loaded" value of the entry by replacing it in the hashmap
                        ArrayList<Object> valSet = new ArrayList<Object>();
                        valSet.add(tableName);
                        valSet.add(colNames);
                        valSet.add(true);
                        strfullPath.put(filePath, valSet);

                    } catch (IOException ex) {
                        Logger.getLogger(DAOCsv.class.getName()).log(Level.SEVERE, null, ex);
                        throw new DAOException(ex);
                    }
                    dataSource.addAnnotation(SpicyEngineConstants.LOADED_INSTANCES_FLAG, true);
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DAOCsv.class.getName()).log(Level.SEVERE, null, ex);
            throw new DAOException(ex);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (connection != null)
                connection.close();
        }
    }

    private static void setup() {
        for (int i=EXP_INDEX; i<= EXP_INDEX; i++) {
            File dir = new File(EXP_1 + "\\" + (int) Math.pow(2, i));
            File[] csvs = dir.listFiles((dir1, filename) -> filename.endsWith(".csv"));
            for (File csv: csvs) {
                try {

                    Files.copy(csv.toPath(), Paths.get(EXP_1 + "\\" + (int) Math.pow(2, i) + "\\union\\" + csv.getName()), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            runQuery("DROP SCHEMA source1 CASCADE;", IVMUtility.obtainAccessConfiguration());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void runQuery(final String query, final AccessConfiguration accessConfiguration) throws SQLException {
        Connection connection = null;
        try {
            connection = new SimpleDbConnectionFactory().getConnection(accessConfiguration);
        } catch (DAOException e) {
            e.printStackTrace();
        }
        Statement statement;
        try {
            if (connection != null) {
                statement = connection.createStatement();
                statement.execute(query);
                connection.close();
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static IDataSourceProxy obtainDeltaSource(String experiment, int splits, int j) {
        try {
            return new DAOMappingTask().loadMappingTask(2,
                    experiment + "\\" + splits + "\\d_" + j + "\\map.xml",
                    SpicyEngineConstants.LINES_BASED_MAPPING_TASK,
                    false).getSourceProxy();
        } catch (DAOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static MappingTask obtainUnionTask(final String experiment, final int splits) {
        try {
            return new DAOMappingTask().loadMappingTask(1,
                    experiment + "\\" + splits + "\\union\\map.xml",
                    SpicyEngineConstants.LINES_BASED_MAPPING_TASK,
                    false);
        } catch (DAOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void unifySourceData(String experiment, int splits, int j) {
        File dir = new File(experiment + "\\" + splits + "\\d_" + j);
        //Fix spaces to _

        File[] csvs = dir.listFiles((dir1, filename) -> filename.endsWith(".csv"));
        if (csvs != null) {
            for (File csv: csvs) {
                Path path = Paths.get(experiment + "\\" + splits + "\\d_" + j + "\\" +csv.getName());
                try {
                    List<String> lines = Files.readAllLines(path);
                    lines.remove(0);
                    Path writePath = Paths.get(experiment + "\\" + splits + "\\union" + "\\" +csv.getName());

                    Files.write(writePath, lines, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

    }


    private static void unifySourceData2(String experiment, int splits, int j) {

        try {

            File dir = new File(experiment + "\\" + splits + "\\d_" + j);
            //Fix spaces to _

            File[] csvs = dir.listFiles((dir1, filename) -> filename.endsWith(".csv"));
            for (File csv : csvs) {
                Reader d = Files.newBufferedReader(Paths.get((experiment + "\\" + splits + "\\d_" + j + "\\" + csv.getName()).replaceFirst("/C:","")));
                CSVParser p = new CSVParser(d, CSVFormat.DEFAULT.withHeader());

                Writer w = Files.newBufferedWriter(
                        Paths.get((experiment + "\\" + splits + "\\union" + "\\" + csv.getName()).replaceFirst("/C:","")),
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                CSVPrinter csvPrinter = new CSVPrinter(w, CSVFormat.DEFAULT);
               // Path path = Paths.get(experiment + "\\" + splits + "\\d_" + j + "\\" + csv.getName());
                try {
                    boolean b = false;
                   for (CSVRecord record: p) {
                       if (!b) {
                           b=true;
                           //csvPrinter.print("");
                       }
                       csvPrinter.printRecord(record);
                   }
                   d.close();
                   p.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        /*if (csvs != null) {
            for (File csv : csvs) {
                Path path = Paths.get(experiment + "\\" + splits + "\\d_" + j + "\\" + csv.getName());
                try {
                    List<String> lines = Files.readAllLines(path);
                    lines.remove(0);
                    lines.set(0, "\r\n" +lines.get(0));
                    Path writePath = Paths.get(experiment + "\\" + splits + "\\union" + "\\" + csv.getName());

                    Files.write(writePath, lines, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }*/
    }


}
