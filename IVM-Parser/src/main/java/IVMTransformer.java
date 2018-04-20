import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


class IVMTransformer {
    static String transformQuery(String query, List<String> tablesOfInterest) {
        final Statement statement ;

        try {
            statement = CCJSqlParserUtil.parse(query);
        } catch (JSQLParserException e) {
            throw new RuntimeException("Query can't be parsed.", e);
        }
        final Select select = (Select) statement;
        return getIncrementalQuery(select, tablesOfInterest).toString();
    }

    private static Select getIncrementalQuery(Select select, List<String> tablesOfInterest) {
        final SelectBody selectBody = select.getSelectBody();

        if (!Collections.disjoint(tablesOfInterest, new TablesNamesFinder().getTableList(select))) {
            if (selectBody instanceof SetOperationList){
                final SetOperationList setOperations = (SetOperationList) selectBody;
                //TODO Incremental Union, Minus
                setOperations.getPlainSelects();

            } else if (selectBody instanceof PlainSelect) {
                final PlainSelect plainSelect  = (PlainSelect) selectBody;
                if (!(plainSelect.getSelectItems().get(0).toString().equals("*"))) {
                    return incrementalProjection(plainSelect, tablesOfInterest);
                }
                else if (!plainSelect.getJoins().isEmpty()) {
                    return incrementalInnerJoin(plainSelect, tablesOfInterest);
                }

                else {
                   select.setSelectBody(incrementalSingle(plainSelect.getFromItem(), tablesOfInterest));
                   return select;
                }
            }
        }

        return select;
    }

    private static PlainSelect incrementalSingle(FromItem fromItem, List<String> tablesOfInterest) {

        if (fromItem instanceof Table) {
            PlainSelect p = new PlainSelect();
            p.setFromItem(incrementalIdentity(fromItem, tablesOfInterest));
            return p;

        }
        else if (fromItem instanceof SubSelect){
            final SubSelect subSelect = (SubSelect) fromItem;

            final Select sub = new Select();
            sub.setSelectBody(subSelect.getSelectBody());
            if (!Collections.disjoint(tablesOfInterest, new TablesNamesFinder().getTableList(sub))) {
                return (PlainSelect) getIncrementalQuery(sub, tablesOfInterest).getSelectBody();
            }
            else {
                return (PlainSelect) subSelect.getSelectBody();
            }
        }
        return null;
    }


    private static Select incrementalProjection(PlainSelect selectFrom, List<String> tablesOfInterest) {
        final SetOperationList finalProjection = new SetOperationList();
        final List<PlainSelect> listOfPlainSelects = new ArrayList<PlainSelect>();

        listOfPlainSelects.add(incrementalSingle(selectFrom.getFromItem(), tablesOfInterest));
        listOfPlainSelects.add(selectFrom);
        final List<SetOperation> ops = new ArrayList<SetOperation>();
        ops.add(new MinusOp());
        finalProjection.setOpsAndSelects(listOfPlainSelects, ops);

        final Select projection = new Select();
        projection.setSelectBody(finalProjection);
        return projection;
    }

    private static FromItem incrementalIdentity(FromItem table, List<String> tablesOfInterest) {
        for (final String incTable: tablesOfInterest) {
            if (table.toString().equals(incTable)) {
                return new Table("Delta_"+table.toString());
            }
        }
        return table;
    }

    private static Select incrementalInnerJoin(PlainSelect selectFrom, List<String> tablesOfInterest) {
        final FromItem s = selectFrom.getFromItem();
        final FromItem t = selectFrom.getJoins().get(0).getRightItem();

        final Select innerJoin = new Select();
        final SetOperationList unionList = new SetOperationList();
        final List<PlainSelect> unionParts = new ArrayList<PlainSelect>();

        final PlainSelect lhs = new PlainSelect();
        lhs.setFromItem(mod(s, tablesOfInterest));
        final List<Join> joinsL = new ArrayList<Join>();
        final Join join = new Join();
        join.setInner(true);

        //is t a table of subselect?
        join.setRightItem(incrementalSingle(t, tablesOfInterest).getFromItem());


        joinsL.add(join);
        lhs.setJoins(joinsL);
        unionParts.add(lhs);


        final PlainSelect rhs = new PlainSelect();
        rhs.setFromItem(incrementalSingle(s, tablesOfInterest).getFromItem());
        final List<Join> joinsR = new ArrayList<Join>();
        final Join join2 = new Join();
        join2.setInner(true);

        //is t a table of subselect?
        join2.setRightItem(mod(t, tablesOfInterest));


        joinsR.add(join2);
        rhs.setJoins(joinsR);

        unionParts.add(rhs);
        final UnionOp union = new UnionOp();
        final List<SetOperation> setOps = new ArrayList<SetOperation>();
        setOps.add(union);
        unionList.setOpsAndSelects(unionParts, setOps);
        innerJoin.setSelectBody(unionList);

        return innerJoin;

    }

    private static FromItem mod(FromItem r, List<String> tablesOfInterest) {
        final SetOperationList unionList = new SetOperationList();

        final UnionOp union = new UnionOp();
        final List<PlainSelect> unionParts = new ArrayList<PlainSelect>();
        final List<SetOperation> setOps = new ArrayList<SetOperation>();
        setOps.add(union);

        final PlainSelect firstPlainSelect = new PlainSelect();
        firstPlainSelect.setFromItem(r);
        unionParts.add(firstPlainSelect);
        PlainSelect secondPlainSelect = new PlainSelect();
        secondPlainSelect.setFromItem(incrementalSingle(r, tablesOfInterest).getFromItem());
        unionParts.add(secondPlainSelect);


        unionList.setOpsAndSelects(unionParts, setOps);
        SubSelect modQuery = new SubSelect();
        modQuery.setSelectBody(unionList);




        return  modQuery;
    }
}
