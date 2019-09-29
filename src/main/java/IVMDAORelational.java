import it.unibas.spicy.model.datasource.Duplication;
import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.datasource.SelectionCondition;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.relational.DAORelational;
import it.unibas.spicy.persistence.relational.DBFragmentDescription;
import it.unibas.spicy.persistence.relational.IConnectionFactory;
import it.unibas.spicy.persistence.relational.SimpleDbConnectionFactory;
import it.unibas.spicy.utility.SpicyEngineConstants;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class IVMDAORelational extends DAORelational {

    private final int deltaNo;

    public IVMDAORelational(final int deltaSchemaNumber) {
        this.deltaNo = deltaSchemaNumber;
    }


    public void loadInstance(int scenarioNo,
                             AccessConfiguration accessConfiguration,
                             IDataSourceProxy dataSource,
                             DBFragmentDescription dataDescription,
                             IConnectionFactory dataSourceDB, boolean source) throws DAOException, SQLException {
        final boolean loaded = dataSource.getAnnotation(SpicyEngineConstants.LOADED_INSTANCES_FLAG) != null
                && (boolean)dataSource.getAnnotation(SpicyEngineConstants.LOADED_INSTANCES_FLAG);
        Connection connection = new SimpleDbConnectionFactory().getConnection(accessConfiguration);
        Statement statement = connection.createStatement();
        if (!loaded) {
            super.loadInstance(scenarioNo, accessConfiguration, dataSource, dataDescription, dataSourceDB, source);
            loadClones(dataSource, statement);
            //addIndexesForJoins(dataSource, statement);
            //addIndexesForSelections(dataSource, statement);
        }
        else {

            final List<String> relations = IVMUtility.getRelationLabels(dataSource);
            for (final String relation: relations) {
                if (relation.contains("dataSource1_")) {
                    statement.executeUpdate("DELETE FROM source1.\"" + relation +"\";");

                }
                final String original = relation.replaceAll("dataSource[0-1]_", "").replaceAll("_[0-9]_", "");
                statement.executeUpdate("INSERT INTO source1.\"" + relation + "\" select distinct * from source" + this.deltaNo +".\"" +original + "\";" );
            }
        }
        connection.close();
        statement.close();
    //if not loaded use super method + add indexes on joins

    //if loaded, incrementally update using deltaSource
    }

    private void addIndexesForSelections(IDataSourceProxy dataSource, Statement statement) throws SQLException {
        for (SelectionCondition s: dataSource.getSelectionConditions()) {
            String relation = s.getSetPaths().get(0).getLastStep();
            if (relation.contains("dataSource0_")) {
                String column = StringUtils.substringBetween(s.getCondition().toString(), "Tuple."," ");
                statement.executeUpdate("create index " + relation + "_" + column +" on source1.\""+relation + "\"(\"" + column +"\");");
            }

        }
    }

    private void addIndexesForJoins(IDataSourceProxy dataSource, Statement statement) throws SQLException {
        for (final JoinCondition join:  dataSource.getJoinConditions()) {
            final List<String> fromPath =  join.getFromPaths().get(0).getPathSteps();
            final List<String> toPath =  join.getToPaths().get(0).getPathSteps();
             String relation;
             String column;
            //if (fromPath.get(1).contains("dataSource0_")) {
                relation = fromPath.get(1);
                column = fromPath.get(3);
            statement.executeUpdate("create  index " + relation + "_" + column +" on source1.\""+relation + "\"(\"" + column +"\");");

            //}
            //else {
                relation = toPath.get(1);
                column = toPath.get(3);
            //}
            statement.executeUpdate("create  index " + relation + "_" + column +" on source1.\""+relation + "\"(\"" + column +"\");");
        }
    }

    private void loadClones(IDataSourceProxy dataSource, Statement statement) throws SQLException {
        for (final Duplication duplication: dataSource.getDuplications()) {
            statement.executeUpdate("CREATE TABLE source1.\"" + duplication.getClonePath().getLastStep() +"\" " +
                    "AS select distinct * from source1.\"" + duplication.getOriginalPath().getLastStep() +"\";");
        }
    }

}
