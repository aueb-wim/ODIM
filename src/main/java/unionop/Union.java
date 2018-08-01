package unionop;

import it.unibas.spicy.model.algebra.operators.IAlgebraTreeVisitor;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.paths.VariablePathExpression;
import it.unibas.spicy.utility.SpicyEngineUtility;

import java.util.List;

/**
 * TODO: Repackage engine with the new Union Operator.
 */

public class Union extends IntermediateOperator {

    //private static Log logger = LogFactory.getLog(Union.class);

    private List<VariablePathExpression> leftPaths;
    private List<VariablePathExpression> rightPaths;

    public Union(final List<VariablePathExpression> leftPaths, final List<VariablePathExpression> rightPaths) {
        this.leftPaths = leftPaths;
        this.rightPaths = rightPaths;
    }

    public String getName() {
        return "union";
    }

    public IDataSourceProxy execute(final IDataSourceProxy dataSource) {
        if (result != null) {
            return result; //This is some sort of caching used by the engine: I choose to stick to it (for now).
        }

        final IDataSourceProxy leftChild = children.get(0).execute(dataSource);
        final IDataSourceProxy rightChild = children.get(1).execute(dataSource);
        final IDataSourceProxy unionResult = calculateUnion(leftChild, rightChild, leftPaths, rightPaths);

        result = unionResult;   //Caching.
        return unionResult;
    }

    public void accept(IAlgebraTreeVisitor visitor) {
        //visitor.visitUnion(this);   //TODO: Visitor Pattern! Essentially identical to Intersection.
    }

    private IDataSourceProxy calculateUnion(final IDataSourceProxy leftOperand,
                                            final IDataSourceProxy rightOperand,
                                            final List<VariablePathExpression> leftPaths,
                                            final List<VariablePathExpression> rightPaths) {
        final IDataSourceProxy clone = SpicyEngineUtility.cloneAlgebraDataSource(leftOperand);
        for (int i = 0; i < clone.getInstances().size(); i++) {
            final INode leftInstance = clone.getInstances().get(i);
            final INode rightInstance = rightOperand.getInstances().get(i);

            final List<INode> childrenOfLeft = leftInstance.getChildren();
            final List<INode> childrenOfRight = rightInstance.getChildren();

            for (int j = 0; i < childrenOfLeft.size(); j++) {
                final String leftTupleSummary = generateSummary(childrenOfLeft.get(j), leftPaths);
                for (final INode rightChild : childrenOfRight) {
                    if (!leftTupleSummary.equals(generateSummary(rightChild, rightPaths))) {
                        childrenOfLeft.add(rightChild);
                    }
                }
            }
        }
        return clone;
    }

    private String generateSummary(INode tuple, List<VariablePathExpression> paths) {
        StringBuilder summary = new StringBuilder("#");
        for (VariablePathExpression path : paths) {
            String value = SpicyEngineUtility.findAttributeValue(tuple, path.toString()).toString();
            summary.append(value).append("#");
        }
        return summary.toString();
    }

}
