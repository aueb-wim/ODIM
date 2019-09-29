import it.unibas.spicy.model.algebra.query.operators.sql.GenerateSQL;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.utility.SpicyEngineConstants;
import mipmapreduced.DbConnector;
import mipmapreduced.InstanceTranslator;

//0 C:\Users\Artem\OneDrive\Desktop\IVM-NEW\src\main\resources\postgresdb.properties
//1 "C:\\Users\\Artem\\OneDrive\\Desktop\\IVM-NEW\\src\\main\\resources\\map.xml
//2 "C:\\Users\\Artem\\OneDrive\\Desktop\\IVM-NEW\\src\\main\\resources\\old\\map.xml


class Testbed {
    public static void main(String[] args) {
        final String confFile = args[0];
        final String taskFile = args[1];//"C:\\Users\\Artem\\OneDrive\\Desktop\\MIPMAP\\MIPMap\\MIPMap_tests\\Datasets\\DS1\\test_run.xml";;
        final String oldTaskFile = args[2]; //"C:\\Users\\Artem\\OneDrive\\Desktop\\MIPMAP\\MIPMap\\MIPMap_tests\\Datasets\\DS1\\old\\old_test.xml";//args[2];
        new DbConnector().configureDatabaseProperties(confFile);

        MappingTask task = null;
        try {
            task = new DAOMappingTask().loadMappingTask(
                    1,
                    taskFile,
                    SpicyEngineConstants.LINES_BASED_MAPPING_TASK,
                    false);
        } catch (DAOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        try {
            ///System.out.println(task.getMappingData().getRewrittenRules());
           final MappingTask ivmTask = new IncrementalMappingTask(task, obtainDeltaData(oldTaskFile), false);
           // System.out.println(ivmTask.getMappingData().getRewrittenRules());
            //System.out.println(task.getConfig());
            //System.out.println(ivmTask.getConfig());
            new InstanceTranslator().performAction(ivmTask, false, null);
            System.out.println(new GenerateSQL().generateSQL(ivmTask, 1).replaceAll("(?m)^delete from.*", "")); //remove all lines with delete from

            //new DAOMappingTaskLines().saveMappingTask(ivmTask, "C:\\Users\\Artem\\OneDrive\\Desktop\\ivm.xml");
            //final MappingTask t = new DAOMappingTask().loadMappingTask(1,
             //       "C:\\Users\\Artem\\OneDrive\\Desktop\\ivm.xml",
               //     SpicyEngineConstants.LINES_BASED_MAPPING_TASK,
                 //   false);
            //new InstanceTranslator().performAction( new DAOMappingTask().loadMappingTask(
             //       1,
              //    "C:\\Users\\Artem\\OneDrive\\Desktop\\IVM-NEW\\src\\main\\resources\\union\\map.xml",
               //     SpicyEngineConstants.LINES_BASED_MAPPING_TASK,
                 //   false), false, "C:\\Users\\Artem\\OneDrive\\Desktop\\IVM-NEW\\src\\main\\resources");

        } catch (Exception e){//SQLException | DAOException | IOException e) {
            e.printStackTrace();
        }
    }


    private static IDataSourceProxy obtainDeltaData(final String oldTaskFile) {
        try {
            return new DAOMappingTask().loadMappingTask(1,
                    oldTaskFile,
                    SpicyEngineConstants.LINES_BASED_MAPPING_TASK,
                    false).getSourceProxy();
        } catch (DAOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
