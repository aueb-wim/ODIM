import it.unibas.spicy.model.algebra.query.operators.sql.GenerateSQL;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.persistence.relational.DBFragmentDescription;
import it.unibas.spicy.persistence.relational.IConnectionFactory;
import it.unibas.spicy.persistence.relational.SimpleDbConnectionFactory;
import it.unibas.spicy.utility.SpicyEngineConstants;
import mipmapreduced.DbConnector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class IVMEXPERIMENT {

    private final static String EXP_1 = "C:" + File.separator +"Users" + File.separator + "Artem" + File.separator + "OneDrive" + File.separator +
            "Desktop" + File.separator + "MIPMAP" +File.separator  + "EXPERIMENTS" +File.separator + "remappingtasks" +File.separator + "freiburg_map_Artemis";
    private final static String dbResources = "C:\\Users\\Artem\\OneDrive\\Desktop\\IVM-NEW\\src\\main\\resources\\postgresdb.properties";
    private static final short EXP_INDEX = 2;

    public static void main(String[] args) {
        new DbConnector().configureDatabaseProperties(dbResources);
        Connection connection = null;
        try {
            connection = new SimpleDbConnectionFactory().getConnection(IVMUtility.obtainAccessConfiguration());
        } catch (DAOException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = connection.createStatement();


            setup();
            runQuery("DROP SCHEMA source1 CASCADE;",statement);

            IVMDAORelational dao = new IVMDAORelational(2);
            MappingTask task = obtainUnionTask(EXP_1, (int) Math.pow(2, EXP_INDEX));
            IDataSourceProxy deltaSource = obtainDeltaSource(EXP_1, (int) Math.pow(2, EXP_INDEX), 1);
            try {
                new DAOCsv().loadInstance(2, deltaSource, true);
            } catch (DAOException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                final MappingTask ivmTask = new IncrementalMappingTask(task, deltaSource, false);
                final String sqlQuery = new GenerateSQL().generateSQL(ivmTask, 1).replaceAll("drop table if exists source1(.*\\s*.*\\s*.*\\s*.*)", "");//extend it!
                AccessConfiguration conf = (AccessConfiguration) ivmTask.getSourceProxy().getAnnotation("access configuration");
                DBFragmentDescription dataDescription = new DBFragmentDescription();
                IConnectionFactory dataSourceDB = new SimpleDbConnectionFactory();
                long start = System.nanoTime();
                dao.loadInstance(1, conf, ivmTask.getSourceProxy(), dataDescription, dataSourceDB, true);
                runQuery(sqlQuery, statement);
                System.out.println("ivm: 1 " + (double) (System.nanoTime() - start) / 1000000000);
                // System.out.println(sqlQuery);
                for (int j = 2; j < (int) Math.pow(2, EXP_INDEX); j++) {
                    deltaSource = obtainDeltaSource(EXP_1, (int) Math.pow(2, EXP_INDEX), 1);
                    new DAOCsv().loadInstance(2, deltaSource, true);

                    start = System.nanoTime();
                    //final long t = System.nanoTime();
                    dao.loadInstance(1, conf, ivmTask.getSourceProxy(), dataDescription, dataSourceDB, true);
                    //System.out.println("load: " + (double) (System.nanoTime() - t) / 1000000000);
                    //System.out.println("loading: " + (double)(System.nanoTime() -start)/1000000000);
                    //start = System.nanoTime();
                    runQuery(sqlQuery, statement);//
                    System.out.println( (double) (System.nanoTime() - start) / 1000000000);

                }
            } catch (SQLException | DAOException e) {
                e.printStackTrace();
            }
        }
     catch (SQLException e) {
        e.printStackTrace();
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

    }

    private static void runQuery(final String query, Statement statement) throws SQLException {

        try {

            statement.execute(query);


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

}
