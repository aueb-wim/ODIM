//Following the paradigm of MIPMAP-Reduced.

import it.unibas.spicy.model.mapping.FORule;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOHandleDB;
import it.unibas.spicy.persistence.DAOMappingTask;
import it.unibas.spicy.utility.SpicyEngineConstants;
import mipmapreduced.DbConnector;
import mipmapreduced.DirectoryChecker;
import mipmapreduced.TaskHandler;
import org.apache.commons.io.FilenameUtils;


import java.util.List;

public class Testbed {
    public static void main(String[] args) {
        final MappingTask mappingTask = obtainMappingTask(args);
        final List<FORule> coreIncrementalTGDs = new TGDRewriter(mappingTask).rewrite();
    }

    private static MappingTask obtainMappingTask(String[] args){
        String absoluteMappingTaskFilepath = FilenameUtils.separatorsToSystem(args[0]);
        String dbConfFile = FilenameUtils.separatorsToSystem(args[1]);
        String exportCommand = FilenameUtils.separatorsToSystem(args[2]);
        String exportDatabaseConfig = null;
        String exportPath = null;
        boolean pkConstraints = true;
        DirectoryChecker checker = new DirectoryChecker();
        if(exportCommand.equals("-db")){
            exportDatabaseConfig = FilenameUtils.separatorsToSystem(args[3]);
        }
        else {
            System.out.println("Wrong export command!");
            System.exit(-1);
        }
        if (checker.checkFileValidity(absoluteMappingTaskFilepath) && checker.checkFileValidity(dbConfFile)) {
            DbConnector dbconnect = new DbConnector();
            dbconnect.configureDatabaseProperties(dbConfFile);

            TaskHandler handleMappingTask = new TaskHandler(absoluteMappingTaskFilepath, exportCommand, exportPath,
                    exportDatabaseConfig, pkConstraints);
            createDB();
            DAOMappingTask daoMappingTask = new DAOMappingTask();
            MappingTask mappingTask = null;
            try {
                mappingTask = daoMappingTask.loadMappingTask(1,absoluteMappingTaskFilepath,
                        SpicyEngineConstants.LINES_BASED_MAPPING_TASK, false);
            } catch (DAOException  ex) {
                System.out.println(ex);
                System.exit(-1);
            }

            return mappingTask;
        }
        else {
            System.out.println("\nInvalid path input or the file/path does not exist: " + checker.getInvalidFilePath());
            System.exit(-1);
        }
        return null;
    }

    private static void createDB(){
        DAOHandleDB daoCreateDB = new DAOHandleDB();
        try {
            daoCreateDB.createNewDatabase();
        } catch (DAOException ex) {
            System.out.println(ex);
            System.exit(-1);
        }
    }


}
