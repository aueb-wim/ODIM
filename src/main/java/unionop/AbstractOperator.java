package unionop;

import it.unibas.spicy.model.algebra.IAlgebraOperator;
import it.unibas.spicy.model.algebra.operators.AlgebraTreeToString;
import it.unibas.spicy.model.mapping.IDataSourceProxy;


 abstract class AbstractOperator implements IAlgebraOperator {
    protected IAlgebraOperator father;
    protected IDataSourceProxy result;
    protected String id;

    public IAlgebraOperator getFather() {
        return father;
    }

    public void setFather(IAlgebraOperator father) {
        this.father = father;
    }

    public String getId() {
        return id;
    }

    public String printIds() {
        StringBuilder allIds = new StringBuilder();
        if (this.id != null) {
            allIds.append("\n").append(id).append("\n");
        }
        IAlgebraOperator ancestor = this.father;
        while (ancestor != null) {
            if (ancestor.getId() != null) {
                allIds.insert(0, ancestor.getId() + "\n");
            }
            ancestor = ancestor.getFather();
        }
        return allIds.toString();
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return new AlgebraTreeToString().treeToString(this);
    }
}
