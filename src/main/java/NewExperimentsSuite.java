import it.unibas.spicy.model.algebra.query.operators.sql.GenerateSQL;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTask;
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

public class NewExperimentsSuite {

    private final static String EXP_1 = "C:\\Users\\Spyros\\Desktop\\EXPERIMENTS\\remappingtasks\\ppmi_mipmap_Artemis";
    private final static String dbResources = "C:\\Users\\Spyros\\Desktop\\IVM-NEW\\src\\main\\resources\\postgresdb.properties";
    private static final short EXP_INDEX = 4;


  /*  public static void main(String[] args) {
        new DbConnector().configureDatabaseProperties(dbResources);
        setup();
        IVMDAORelational dao = new IVMDAORelational();
        MappingTask task = obtainUnionTask(EXP_1, (int) Math.pow(2,EXP_INDEX));
        IDataSourceProxy deltaSource = obtainDeltaSource(EXP_1, (int) Math.pow(2,EXP_INDEX), 1);
        try {
            final MappingTask ivmTask = new IncrementalMappingTask(task, deltaSource, false);
            final String sqlQuery = new GenerateSQL().generateSQL(ivmTask,1); //extend it!
            for (int j = 2; j < (int) Math.pow(2, EXP_INDEX); j++) {
                AccessConfiguration conf = (AccessConfiguration)ivmTask.getSourceProxy().getAnnotation("access configuration");
                DBFragmentDescription dataDescription = new DBFragmentDescription();
                IConnectionFactory dataSourceDB = new SimpleDbConnectionFactory();
                long start = System.nanoTime();
                dao.loadInstance(1, conf, ivmTask.getSourceProxy(),  dataDescription, dataSourceDB, true);
                System.out.println("loading: " + (double)(System.nanoTime() -start)/1000000000);
                start = System.nanoTime();
                //runQuery();//
                System.out.println("ivm: " +  (double)(System.nanoTime() -start)/1000000000);
                obtainDeltaSource(EXP_1, (int) Math.pow(2,EXP_INDEX), 1);
                dao.updateSource();

            }
        } catch (SQLException | DAOException e) {
            e.printStackTrace();
        }

    }

    private static void setup() {
        for (int i=4; i<= EXP_INDEX; i++) {
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
            runQuery("DROP SCHEMA source1 CASCADE;",IVMUtility.obtainAccessConfiguration());
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
                statement.executeUpdate(query);
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            connection.close();
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
    }*/

}
