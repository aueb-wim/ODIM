import it.unibas.spicy.model.mapping.FORule;
import it.unibas.spicy.model.mapping.MappingData;
import it.unibas.spicy.model.mapping.MappingTask;

import java.util.List;

public class TGDRewriter {

    private final MappingTask mappingTask;

    public TGDRewriter(final MappingTask mappingTask) {
        this.mappingTask = mappingTask;
    }

    public List<FORule> rewrite() {
        final MappingData mappingData = this.mappingTask.getMappingData();
        List<FORule> tgds = mappingData.getCandidateSTTgds();
        transformTGDsToIncremental(tgds);   //TODO Engine Flag + boilerplate.
        if (mappingTask.getConfig().rewriteOverlaps()) {
            tgds =  mappingData.getOverlapSTTgds(); //TODO Refactor! The List handling inside is really bad.
        }
        if (!mappingData.hasSelfJoinsInTgdConclusions()) {
            //TODO Subsumptions and Coverages
        }
        else {
            throw new RuntimeException("This feature has not been implemented yet.");
        }
        return tgds;
    }

    private void transformTGDsToIncremental(final List<FORule> tgds) {
        for (int i=0; i < tgds.size(); i++) {
            tgds.set(i, obtainIncrementalTgd(tgds.get(i)));
        }
    }


    private FORule obtainIncrementalTgd(FORule tgd) {
        //final IAlgebraOperator algebraTreeRoot = tgd.getTargetView().getAlgebraTree();

        return tgd; //TODO Return incremental tgd.
    }




}
