import it.unibas.spicy.model.correspondence.ValueCorrespondence;

import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.persistence.DAOException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IncrementalMappingTask extends MappingTask {

    private List<JoinCondition> joins;

    /**
     * An IncrementalMappingTask is created based on an original MIPMAP MappingTask and an IDataSourceProxy that
     * contains incremental additions that the user wants to add to the IDataSourceProxy of the original MappingTask.
     * @param originalTask
     * @param deltaSourceProxy
     * @param schemaExists whether the schema of tha mapping task exists in a relational form.
     * @throws SQLException
     * @throws DAOException
     */
    public IncrementalMappingTask(final MappingTask originalTask,
                           final IDataSourceProxy deltaSourceProxy,
                           final boolean schemaExists) throws SQLException, DAOException {
        super(originalTask.getSourceProxy(), originalTask.getTargetProxy(), originalTask.getType());
        this.joins = originalTask.getSourceProxy().getJoinConditions();
        final IDataSourceProxy originalProxy = originalTask.getSourceProxy();
        this.addSource(new IVMDataSourceProxy(originalProxy, deltaSourceProxy, schemaExists));
        this.addTarget(originalTask.getTargetProxy());
        this.setModified(true);
        this.getConfig().setRewriting(originalTask.getConfig());

        for (final ValueCorrespondence c : originalTask.getValueCorrespondences()) {
            this.addCorrespondence(c);
        }
    }

    private PathExpression getIVMSourcePath(final PathExpression sourcePath, final int i) {

        final List<String> newPath = new ArrayList<>();
        newPath.add("mipmaptask");
        newPath.add("dataSource" + i + "_" + sourcePath.getPathSteps().get(1));
        newPath.add("dataSource" + i + "_" + sourcePath.getPathSteps().get(2));
        newPath.add(sourcePath.getLastStep());
        return new PathExpression(newPath);
    }

    private Expression getIVMTransformationFunction(ValueCorrespondence c, int i) {

        final String oldExp = c.
                getTransformationFunction().
                getJepExpression().
                toStringWithDollars();

        return new Expression(removeUnderscores(oldExp).replaceAll("\\$.*?\\.(.*?)\\.(.*?)\\.",
                "mipmaptask.dataSource" + i + "_$1.dataSource" + i + "_$2."));

    }

    private String removeUnderscores(final String text) {//TODO: Do we need the while loop?
        String newText = text;
        while (!newText.replaceAll("_([A-Za-z0-9_$]+)\\((.*?)\\)", "$1($2)").equals(newText)) {
            newText = newText.replaceAll("_([A-Za-z0-9_$]+)\\((.*?)\\)", "$1($2)");
        }
        return newText;
    }

    @Override
    public void addCorrespondence(final ValueCorrespondence c) {
        final List<PathExpression> sourcePaths = c.getSourcePaths();
        if (sourcePaths != null && (sourcePaths.size() == 1 ||
                sourcePaths.get(0).getPathSteps().get(1).equals(sourcePaths.get(1).getPathSteps().get(1)))) {
            final List<PathExpression> ivmPaths = new ArrayList<>();
            for (final PathExpression path : sourcePaths) {
                ivmPaths.add(getIVMSourcePath(path, 1));
            }
            super.addCorrespondence(new ValueCorrespondence(
                    ivmPaths,
                    c.getTargetPath(),
                    getIVMTransformationFunction(c, 1)));

            if (hasJoinsOriginally( sourcePaths.get(0).getPathSteps().get(1))) {
                final List<PathExpression> ivm2Paths = new ArrayList<>();
                for (final PathExpression path : sourcePaths) {
                    ivm2Paths.add(getIVMSourcePath(path, 0));
                }

                super.addCorrespondence(new ValueCorrespondence(
                        ivm2Paths,
                        c.getTargetPath(),
                        getIVMTransformationFunction(c, 0)));
            }
        } else if (sourcePaths != null && sourcePaths.size() == 2 &&
                   !sourcePaths.get(0).getPathSteps().get(1).equals(sourcePaths.get(1).getPathSteps().get(1))) {

            final List<PathExpression> ivm1Paths = new ArrayList<>();
            ivm1Paths.add(getIVMSourcePath(sourcePaths.get(0), 1));
            ivm1Paths.add(getIVMSourcePath(sourcePaths.get(1), 0));

            final Expression exp = getIVMTransformationFunction(c, 1);
            String fString = exp.toString().replaceAll(
                    "mipmaptask\\.dataSource1_" + sourcePaths.get(1).getPathSteps().get(1)
                            + "\\." + "dataSource1_" + sourcePaths.get(1).getPathSteps().get(1),
                    "mipmaptask.dataSource0_" + sourcePaths.get(1).getPathSteps().get(1)
                            + "." + "dataSource0_" + sourcePaths.get(1).getPathSteps().get(1));

            super.addCorrespondence(new ValueCorrespondence(ivm1Paths, c.getTargetPath(), new Expression(fString)));

            final List<PathExpression> ivm2Paths = new ArrayList<>();
            ivm2Paths.add(getIVMSourcePath(sourcePaths.get(0), 0));
            ivm2Paths.add(getIVMSourcePath(sourcePaths.get(1), 1));


            final Expression exp2 = getIVMTransformationFunction(c, 0);
            String fString2 = exp2.toString().replaceAll(
                    "mipmaptask\\.dataSource0_" + sourcePaths.get(1).getPathSteps().get(1)
                            + "\\." + "dataSource0_" + sourcePaths.get(1).getPathSteps().get(1),
                    "mipmaptask.dataSource1_" + sourcePaths.get(1).getPathSteps().get(1)
                            + "." + "dataSource1_" + sourcePaths.get(1).getPathSteps().get(1));
            super.addCorrespondence(new ValueCorrespondence(ivm2Paths, c.getTargetPath(), new Expression(fString2)));
        } else if (sourcePaths == null) {
            super.addCorrespondence(c);
        } else {
            throw new UnsupportedOperationException(">2 sources");
        }
    }

    private boolean hasJoinsOriginally(String relation) {
        for (final JoinCondition join : this.joins) {

            if (join.getFromPaths().get(0).getPathSteps().get(1).equals(relation)
                    || join.getToPaths().get(0).getPathSteps().get(1).equals(relation)) {
                return true;
            }
        }
        return false;


    }
}

