import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import it.unibas.spicy.model.datasource.DataSource;
import it.unibas.spicy.model.datasource.Duplication;
import it.unibas.spicy.model.datasource.FunctionalDependency;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.datasource.SelectionCondition;
import it.unibas.spicy.model.datasource.nodes.SetCloneNode;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.proxies.AbstractDataSourceProxy;
import it.unibas.spicy.model.mapping.proxies.MergeDataSourceProxy;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.persistence.AccessConfiguration;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.Types;
import it.unibas.spicy.persistence.csv.DAOCsv;
import it.unibas.spicy.persistence.relational.DAORelational;
import it.unibas.spicy.persistence.relational.DBFragmentDescription;
import it.unibas.spicy.persistence.relational.SimpleDbConnectionFactory;
import it.unibas.spicy.utility.SpicyEngineConstants;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class IVMDataSourceProxy extends AbstractDataSourceProxy {

    //TODO: Kwalitee

    private final MergeDataSourceProxy mergedProxy;
    private final IDataSourceProxy sqlProxy;
    private final IDataSourceProxy originalSourceProxy;

    private static final String TABLE_LIST = SpicyEngineConstants.CSV_TABLE_FILE_LIST;
    private static final String INSTANCE_LIST = SpicyEngineConstants.CSV_INSTANCES_INFO_LIST;
    private static final String INSTANCE_PATH_LIST = SpicyEngineConstants.INSTANCE_PATH_LIST;

    private Map<String, List<String>> duplicatesMap;

    IVMDataSourceProxy(final IDataSourceProxy originalSourceProxy,
                       final IDataSourceProxy deltaSourceProxy,
                       final boolean schemaExists) throws SQLException, DAOException {
        //TODO: Return IVM Source Proxy difference exception.
        final List<IDataSourceProxy> l = new ArrayList<>();
        this.duplicatesMap = new HashMap<>();
        l.add(originalSourceProxy);
        l.add(deltaSourceProxy);
        this.mergedProxy = new MergeDataSourceProxy(l);
        this.originalSourceProxy = originalSourceProxy;
        updateAnnotations(originalSourceProxy, deltaSourceProxy);
        final AccessConfiguration accessConfiguration = IVMUtility.obtainAccessConfiguration();

        this.sqlProxy = obtainSQLProxy(originalSourceProxy, accessConfiguration, schemaExists);

        this.addAnnotation(SpicyEngineConstants.ACCESS_CONFIGURATION, accessConfiguration);

        originalSourceProxy.getDuplications().sort(Comparator.comparing(Duplication::getClonePath));
        for (final Duplication duplication: originalSourceProxy.getDuplications()) {
            this.addDuplication(duplication.getOriginalPath(), duplication.getClonePath());
        }

        for (final SelectionCondition selection: originalSourceProxy.getSelectionConditions()) {
            this.addSelectionCondition(selection);
        }

        for (final JoinCondition joinCondition: originalSourceProxy.getJoinConditions()) {
            //TODO: Break if not mandatory, we only support inner joins.
            this.addJoinCondition(joinCondition);
        }
    }

    private IDataSourceProxy obtainSQLProxy(final IDataSourceProxy originalSourceProxy,
                                            final AccessConfiguration accessConfiguration,
                                            final boolean schemaExists) throws DAOException,
                                                                                                  SQLException {
        //TODO: This can be more incremental.
        Connection connection = new SimpleDbConnectionFactory().getConnection(accessConfiguration);
        Statement statement = connection.createStatement();
        if (!schemaExists) {
            loadSchema(statement); // TODO: if necessary. Actually, get low level... don't just remove everything. increment 0 and add 1
        }
        else {
            final Map<String, List> tFilesMap = (HashMap) this.mergedProxy.getAnnotation(SpicyEngineConstants.CSV_INSTANCES_INFO_LIST);
            for (final Map.Entry<String, List> entry : tFilesMap.entrySet()) {
                if (((String) entry.getValue().get(0)).contains("dataSource0_")) {
                    entry.getValue().set(2, true);
                }
            }
        }
        new DAOCsv().loadInstance(0, mergedProxy, true);
        unifyDataWithJoins(originalSourceProxy, statement);
        accessConfiguration.setSchemaName(IVMUtility.IVM_WORK_SCHEMA);

        //statement.executeUpdate( "DROP SCHEMA source1 CASCADE;\n" );//probably not needed
        connection.close();
        statement.close();

        return new DAORelational().loadSchema(
                1,
                accessConfiguration,
                new DBFragmentDescription(),
                new SimpleDbConnectionFactory(),
                true);
    }

    private void unifyDataWithJoins(final IDataSourceProxy originalSourceProxy, Statement statement) throws SQLException {

        final List<INode> relations = IVMUtility.getRelations(originalSourceProxy.getDataSource());
        for (final INode relation: relations) {
            if (hasJoins(relation.getLabel(), originalSourceProxy)) {//EXACT
                final List<String> columns = new ArrayList<>();
                for (final INode col: relation.getChildren().get(0).getChildren()) {
                    columns.add("\"" + col.getLabel() + "\"");
                }
                final String columnsString = String.join(", ", columns);
                final String insertQuery = "INSERT INTO source0.\"dataSource0_"
                        + relation.getLabel() + "\"(" + columnsString +")\n";
                final String selectQuery = "Select  distinct "
                                          + columnsString
                                          + " FROM source0.\"dataSource1_"
                                          + relation.getLabel() +"\"";
                statement.executeUpdate(  insertQuery + selectQuery );
            }
        }
    }

    private boolean hasJoins(final String relation, final IDataSourceProxy originalSourceProxy) {
        for (final JoinCondition join : originalSourceProxy.getJoinConditions()) {

            if (join.getFromPaths().get(0).getPathSteps().get(1).replaceAll("_[0-9]_","").equals(relation)
                    || join.getToPaths().get(0).getPathSteps().get(1).replaceAll("_[0-9]_","").equals(relation)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasJoinsExact(final String relation, final IDataSourceProxy originalSourceProxy) {
        for (final JoinCondition join : originalSourceProxy.getJoinConditions()) {

            if (join.getFromPaths().get(0).getPathSteps().get(1).equals(relation)
                    || join.getToPaths().get(0).getPathSteps().get(1).equals(relation)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void loadSchema(Statement statement) throws SQLException {

        statement.executeUpdate( "DROP SCHEMA IF EXISTS " + IVMUtility.IVM_WORK_SCHEMA + " CASCADE ;\n" );

        String createSchemasQuery = "create schema if not exists "
                                   + SpicyEngineConstants.SOURCE_SCHEMA_NAME
                                   +"0"
                                   + ";\n";
        statement.executeUpdate(createSchemasQuery);

        final Map<String, List> tFilesMap = (HashMap) this.mergedProxy.getAnnotation(SpicyEngineConstants.CSV_INSTANCES_INFO_LIST);
        for (final Map.Entry<String, List> entry : tFilesMap.entrySet()) {
            if (((String) entry.getValue().get(0)).contains("dataSource0_") && !hasJoins(((String) entry.getValue().get(0)).replaceAll("dataSource0_",""), this.originalSourceProxy)) {
                entry.getValue().set(2, true);
                continue;
            }
            String tablefullPath = entry.getKey();

            final String[] nextLineWithSpaces;
            final String[] nextLine;
            final Reader r;
            try {
                r = new FileReader(tablefullPath);
                final CSVReader reader = new CSVReaderBuilder(r).
                                         withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS).
                                         build();
                nextLineWithSpaces = reader.readNext();
                nextLine = new String[nextLineWithSpaces.length];
                for (int j=0; j <nextLineWithSpaces.length; j++) {
                    nextLine[j] = nextLineWithSpaces[j].replaceAll("\\s+","_");
                }

                final StringBuilder columnsBuilder = new StringBuilder();
                for (String aNextLine : nextLine) {
                    //trim and remove quotes
                    String columnName = aNextLine.trim();
                    if (!(columnName.startsWith("\"") && columnName.endsWith("\""))) {
                        columnName = "\"" + columnName + "\"";
                    }
                    final String typeOfColumn = Types.POSTGRES_STRING;
                    columnsBuilder.append(columnName).append(" ").append(typeOfColumn).append(",");
                }
                String columns = columnsBuilder.toString();

                reader.close();
                //take out the last ',' character
                columns = columns.substring(0, columns.length() - 1);

                final String table = SpicyEngineConstants.SOURCE_SCHEMA_NAME + 0 + ".\"" +
                        ((List) ((Map) this.mergedProxy.getAnnotation(SpicyEngineConstants.CSV_INSTANCES_INFO_LIST)).
                                get(tablefullPath)).get(0) + "\"";
                statement.executeUpdate("drop table if exists " + table);
                statement.executeUpdate("create table " + table + " (" + columns + ")");
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void updateAnnotations(final IDataSourceProxy originalSourceProxy, final IDataSourceProxy deltaProxy) {
        this.mergedProxy.setAnnotations(originalSourceProxy.getAnnotations());
        final String tListS = ((List<String>)deltaProxy.getAnnotation(TABLE_LIST)).get(0);
        final String deltaPath = tListS.substring(0, tListS.lastIndexOf("\\"));
        final Map<String, List> dTables = (Map<String, List>) deltaProxy.getAnnotation(INSTANCE_LIST);

        final Map<String, List> mTables = (Map<String, List>) this.mergedProxy.getAnnotation(INSTANCE_PATH_LIST);
        mTables.putAll(dTables);
        for (final Map.Entry<String, List> e : mTables.entrySet()) {
            if (e.getKey().contains(deltaPath)) {
                e.getValue().set(0, "dataSource1_" + e.getValue().get(0));
            }
            else {
                e.getValue().set(0, "dataSource0_" + e.getValue().get(0));
            }
        }
        final Map<String, List> mFiles = (Map<String, List>)this.mergedProxy.getAnnotation(INSTANCE_LIST);
        mFiles.putAll(dTables);
        ((List<String>) this.mergedProxy.getAnnotation(TABLE_LIST)).addAll(dTables.keySet());
    }

    @Override
    public void addSelectionCondition(final SelectionCondition selection) {
        boolean createZeroCopy = false;
        if (hasJoinsExact(selection.getSetPaths().get(0).getLastStep(), this.originalSourceProxy)) {
            createZeroCopy = true;
        }
        final List<PathExpression> ivmPaths = new ArrayList<>();
        final List<PathExpression> ivmPathsZero = new ArrayList<>();
        String conditionString = selection.getCondition().toString();
        for (final PathExpression path: selection.getSetPaths()) {
            final String relation = path.getPathSteps().get(1);
            List<String> ivmPathSteps = path.getPathSteps();
            List<String> ivmPathStepsZero = new ArrayList<>(ivmPathSteps);
            ivmPathSteps.set(0, "mipmaptask");
            ivmPathStepsZero.set(0, "mipmaptask");
            if (this.duplicatesMap.containsKey(ivmPathSteps.get(1))) {
                ivmPathSteps.set(1, this.duplicatesMap.get(ivmPathSteps.get(1)).get(0));
                if (createZeroCopy) {
                    ivmPathStepsZero.set(1, this.duplicatesMap.get(ivmPathStepsZero.get(1)).get(1));
                }


            }
            else {
                ivmPathSteps.set(1, "dataSource1_" + path.getPathSteps().get(1));
                if (createZeroCopy) {
                    ivmPathStepsZero.set(1, "dataSource0_" + ivmPathStepsZero.get(1));
                }

            }

            ivmPaths.add(new PathExpression(ivmPathSteps));
            if (createZeroCopy) {
                ivmPathsZero.add(new PathExpression(ivmPathStepsZero));

            }
            final INode node = this.originalSourceProxy.getIntermediateSchema().getChild(relation);
            if (node instanceof SetCloneNode) {
                final String originalFull = ((SetCloneNode) node).getOriginalNodePath().toString();
                final String originalRelation = originalFull.substring(originalFull.indexOf(".")+1);
                conditionString = conditionString.replaceAll(path.toString() + "\\." + originalRelation,
                        String.join("\\.", ivmPathSteps) + ".dataSource1_" + originalRelation);
            }
            else {
                conditionString = conditionString.replaceAll(path.toString()+"\\." +relation,
                        String.join("\\.", ivmPathSteps) + ".dataSource1_" +relation);
            }
        }

        super.addSelectionCondition(new SelectionCondition(ivmPaths,
                new Expression(conditionString),
                this.getIntermediateSchema()));
        this.sqlProxy.addSelectionCondition(new SelectionCondition(ivmPaths,
                new Expression(conditionString),
                this.getIntermediateSchema()));
        if (createZeroCopy) {
            conditionString = conditionString.replaceAll("dataSource1_", "dataSource0_");
            super.addSelectionCondition(new SelectionCondition(ivmPathsZero,
                    new Expression(conditionString),
                    this.getIntermediateSchema()));
            this.sqlProxy.addSelectionCondition(new SelectionCondition(ivmPathsZero,
                    new Expression(conditionString),
                    this.getIntermediateSchema()));
        }
    }


    @Override
    public INode getIntermediateSchema(){
        return this.sqlProxy.getIntermediateSchema();
    }


    private void addDuplication(PathExpression duplication, PathExpression clonePath) {
        final List<String> steps = duplication.getPathSteps();
        steps.set(0, "mipmaptask");
        steps.set(1, "dataSource1_" + steps.get(1));
        this.sqlProxy.addDuplication(new PathExpression(steps));
        //super.addDuplication(new PathExpression(steps));

        final List<String> l  = new ArrayList<>();
        l.add(this.sqlProxy.getDuplications().get(this.sqlProxy.getDuplications().size()-1).getClonePath().getLastStep());
        this.duplicatesMap.put(clonePath.getLastStep(), l);

        if (hasJoinsExact(clonePath.getLastStep(), this.originalSourceProxy)) {
            final List<String> zeroSteps = new ArrayList<>();
            zeroSteps.add(steps.get(0));
            zeroSteps.add(steps.get(1).replaceAll("dataSource1_", "dataSource0_"));
            this.sqlProxy.addDuplication(new PathExpression(zeroSteps));
           //super.addDuplication(new PathExpression(zeroSteps));
            this.duplicatesMap.get(clonePath.getLastStep()).add(this.sqlProxy.getDuplications().get(this.sqlProxy.getDuplications().size()-1).getClonePath().getLastStep());

        }
    }

    @Override
    public DataSource getDataSource() {
        return this.sqlProxy.getDataSource();
    }

    @Override
    public boolean isModified() {
        return modified;
    }

    @Override
    public String getProviderType() {
        return this.sqlProxy.getProviderType();
    }



    @Override
    public List<JoinCondition> getJoinConditions() {
        return this.sqlProxy.getJoinConditions();
    }

    @Override
    public Boolean addJoinCondition(JoinCondition join) {

        final List<PathExpression> firstFromPaths = new ArrayList<>();
        final List<PathExpression> secondFromPaths = new ArrayList<>();


        for (final PathExpression fromPath: join.getFromPaths()) {
            final  List<String> oldSteps = fromPath.getPathSteps();
            final  List<String> newSteps1 = new ArrayList<>();
            final  List<String> newSteps2 = new ArrayList<>();
            newSteps1.add("mipmaptask");
            if (this.duplicatesMap.containsKey(oldSteps.get(1))) {
                newSteps1.add(this.duplicatesMap.get(oldSteps.get(1)).get(0));
            }
            else {
                newSteps1.add("dataSource1_" + oldSteps.get(1));
            }
            newSteps1.add("dataSource1_" + oldSteps.get(2));
            newSteps1.add(oldSteps.get(3));
            firstFromPaths.add(new PathExpression(newSteps1));

            newSteps2.add("mipmaptask");
            newSteps2.add("dataSource0_" + oldSteps.get(1));
            newSteps2.add("dataSource0_" + oldSteps.get(2));
            newSteps2.add(oldSteps.get(3));
            secondFromPaths.add(new PathExpression(newSteps2));
        }

        final List<PathExpression> firstToPaths = new ArrayList<>();
        final List<PathExpression> secondToPaths = new ArrayList<>();

        for (final PathExpression toPath: join.getToPaths()) {
            final  List<String> oldSteps = toPath.getPathSteps();
            final  List<String> newSteps1 = new ArrayList<>();
            final  List<String> newSteps2 = new ArrayList<>();
            newSteps1.add("mipmaptask");
            if (this.duplicatesMap.containsKey(oldSteps.get(1))) {
                newSteps1.add(this.duplicatesMap.get(oldSteps.get(1)).get(1));
            }
            else {
                newSteps1.add("dataSource0_" + oldSteps.get(1));
            }
            newSteps1.add("dataSource0_" + oldSteps.get(2));
            newSteps1.add(oldSteps.get(3));

            firstToPaths.add(new PathExpression(newSteps1));

            newSteps2.add("mipmaptask");
            newSteps2.add("dataSource1_" + oldSteps.get(1));
            newSteps2.add("dataSource1_" + oldSteps.get(2));
            newSteps2.add(oldSteps.get(3));
            secondToPaths.add(new PathExpression(newSteps2));
        }

        final JoinCondition firstIVMJoin = new JoinCondition(
                firstFromPaths,
                firstToPaths,
                join.isMonodirectional(),
                join.isMandatory(),
                join.isMatchString());
        firstIVMJoin.setMandatory(join.isMandatory());



        final JoinCondition secondIVMJoin = new JoinCondition(
                secondFromPaths,
                secondToPaths,
                join.isMonodirectional(),
                join.isMandatory(),
                join.isMatchString());
        secondIVMJoin.setMandatory(join.isMandatory());


        this.sqlProxy.addJoinCondition(firstIVMJoin);

        this.sqlProxy.addJoinCondition(secondIVMJoin);

        return super.addJoinCondition(firstIVMJoin) && super.addJoinCondition(secondIVMJoin);
    }

    @Override
    public List<FunctionalDependency> getFunctionalDependencies() {
        return this.sqlProxy.getFunctionalDependencies();
    }

    @Override
    public List<Duplication> getDuplications() {
        return this.sqlProxy.getDuplications();
    }

    @Override
    public List<SelectionCondition> getSelectionConditions() {
        return this.sqlProxy.getSelectionConditions();
    }

    @Override
    public List<PathExpression> getInclusions() {
        return this.sqlProxy.getInclusions();
    }

    @Override
    public List<PathExpression> getExclusions() {
        return this.sqlProxy.getExclusions();
    }

}
