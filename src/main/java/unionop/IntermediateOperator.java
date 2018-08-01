package unionop;

import it.unibas.spicy.model.algebra.IAlgebraOperator;

import java.util.ArrayList;
import java.util.List;

abstract class IntermediateOperator  extends AbstractOperator {


    protected List<IAlgebraOperator> children = new ArrayList<IAlgebraOperator>();

    public void addChild(IAlgebraOperator child) {
        children.add(child);
        child.setFather(this);
    }

    public List<IAlgebraOperator> getChildren() {
        return children;
    }
}
