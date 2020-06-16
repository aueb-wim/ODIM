import it.unibas.spicy.model.algebra.query.operators.sql.GenerateSQL;
import it.unibas.spicy.model.datasource.DataSource;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.utility.SpicyEngineConstants;

import java.util.ArrayList;
import java.util.List;

public class IVMUtility {

    static final String IVM_WORK_SCHEMA = "source0";

    static List<INode> getRelations(final DataSource source) {
        return source.getSchema().getChildren();
    }

    static List<String> getRelationLabels(final IDataSourceProxy source) {
        final List<INode> relations =source.getIntermediateSchema().getChildren();
        final List<String> labels = new ArrayList<>();
        for (INode r: relations) {
            labels.add(r.getLabel());
        }
        return labels;
    }


    static AccessConfiguration obtainAccessConfiguration() {
        final AccessConfiguration accessConfiguration = new AccessConfiguration();
        accessConfiguration.setDriver(SpicyEngineConstants.ACCESS_CONFIGURATION_DRIVER);
        final String uri = SpicyEngineConstants.ACCESS_CONFIGURATION_URI + SpicyEngineConstants.MAPPING_TASK_DB_NAME;
        accessConfiguration.setUri(uri);
        accessConfiguration.setLogin(SpicyEngineConstants.ACCESS_CONFIGURATION_LOGIN);
        accessConfiguration.setPassword(SpicyEngineConstants.ACCESS_CONFIGURATION_PASS);
        accessConfiguration.setSchemaName("source1");
        return accessConfiguration;
    }

}
