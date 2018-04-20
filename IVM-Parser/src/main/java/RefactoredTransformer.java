import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class RefactoredTransformer {

    private final List<String> tablesOfInterest;
    private final List<SelectItem> ALLSELECTITEMS;

    private final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private final SecureRandom rnd = new SecureRandom();
    private final int LENGTH=3;

    RefactoredTransformer(List<String> tablesOfInterest) {
        this.tablesOfInterest = tablesOfInterest;
        ALLSELECTITEMS = new ArrayList<SelectItem>();
        ALLSELECTITEMS.add(new AllColumns());
    }

    String transformQuery(final String query) {
        final Statement statement;

        try {
            statement = CCJSqlParserUtil.parse(query);
        } catch (JSQLParserException e) {
            throw new RuntimeException("Query can't be parsed.", e);
        }
        final Select select = (Select) statement;
        return getIncrementalQuery(select).toString();
    }

    private boolean shouldContinue(final Select select) {
        final List<String> tablesPresent = new TablesNamesFinder().getTableList(select);
        boolean allDeltasThere = true;
        for (String table: tablesOfInterest) {
            if (!(table.startsWith("Delta_")) & (!tablesPresent.contains("Delta_"+table) &&
                                                 tablesPresent.contains(table))) {
                allDeltasThere = false;
                break;
            }
        }
        return !Collections.disjoint(tablesOfInterest, tablesPresent) && !allDeltasThere;
    }


    private Select getIncrementalQuery(final Select select) {

        final SelectBody selectBody = select.getSelectBody();

        if (shouldContinue(select)) {

            if (selectBody instanceof SetOperationList) {
                throw new RuntimeException("Not Implemented: SetOps.");

            } else if (selectBody instanceof PlainSelect) {

                final PlainSelect plainSelect = (PlainSelect) selectBody;
                if (!(plainSelect.getSelectItems().get(0).toString().equals("*"))) {
                    //Is it a projection?
                    return incrementalProjection(plainSelect);
                } else if (!plainSelect.getJoins().isEmpty()) {
                    // Is it an inner join?
                    final SubSelect joinSub = new SubSelect();
                    if (joinSub.getAlias() == null) {
                        joinSub.setAlias(new Alias(generateRandomStr()));
                    }
                    joinSub.setSelectBody(incrementalInnerJoin(plainSelect).getSelectBody());
                    plainSelect.setFromItem(joinSub);
                } else {
                    //Is it a singleton? TODO Sigmas!
                    plainSelect.setFromItem(incrementalSingle(plainSelect.getFromItem()));
                }
                select.setSelectBody(plainSelect);
                return select;
            }
        }

        return select;
    }


     Select incrementalProjection(final PlainSelect selectFrom) {

        Select initialSelect=null;
        try {
            initialSelect = (Select) CCJSqlParserUtil.parse(selectFrom.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        final SetOperationList finalProjection = new SetOperationList();
        final List<PlainSelect> listOfPlainSelects = new ArrayList<PlainSelect>();
        final PlainSelect select = new PlainSelect();

        select.setFromItem(incrementalSingle(selectFrom.getFromItem()));
        final List<SelectItem> selectItems = new ArrayList<SelectItem>();
        List<SelectItem> oldItems = selectFrom.getSelectItems();
        if (oldItems.get(0).toString().equals("*")) {
            selectItems.add(new AllColumns());
        }
        else {
            for (SelectItem oldItem: oldItems) {
                System.out.println(oldItem);
                //selectItems.add(
            }
        }

        select.setSelectItems(selectItems);


        listOfPlainSelects.add(select);
        listOfPlainSelects.add((PlainSelect) initialSelect.getSelectBody());
        final List<SetOperation> ops = new ArrayList<SetOperation>();
        ops.add(new ExceptOp());
        finalProjection.setOpsAndSelects(listOfPlainSelects, ops);

        final Select projection = new Select();
        projection.setSelectBody(finalProjection);

        return projection;
    }

    private FromItem incrementalSingle(FromItem fromItem) {

        if (fromItem instanceof Table) {
            return incrementalIdentity(fromItem);
        }
        else if (fromItem instanceof SubSelect){
            final SubSelect subSelect = (SubSelect) fromItem;
            if (subSelect.getAlias() == null) {
                subSelect.setAlias(new Alias(generateRandomStr()));
            }
            final Select sub = new Select();
            sub.setSelectBody(subSelect.getSelectBody());
            final SubSelect subSelectToReturn = new SubSelect();
            subSelectToReturn.setSelectBody(getIncrementalQuery(sub).getSelectBody());
            if (subSelectToReturn.getAlias() == null) {
                subSelectToReturn.setAlias(new Alias(generateRandomStr()));
            }
            return subSelectToReturn;
        }

        return fromItem;
    }

    private String generateRandomStr() {

        StringBuilder sb = new StringBuilder( LENGTH );
        for( int i = 0; i < LENGTH; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();


    }


    private FromItem incrementalIdentity(final FromItem table) {
        for (final String incTable: tablesOfInterest) {
            if (table.toString().equals(incTable)) {
                return new Table("Delta_"+table.toString());
            }
        }
        return table;
    }


    private Select incrementalInnerJoin(final PlainSelect selectFrom) {
        final FromItem s = selectFrom.getFromItem();
        final FromItem t = selectFrom.getJoins().get(0).getRightItem();

        final Select innerJoin = new Select();
        final SetOperationList unionList = new SetOperationList();
        final List<PlainSelect> unionParts = new ArrayList<PlainSelect>();

        final PlainSelect lhs = getSimpleAllSelect(mod(s));
        final List<Join> joinsL = new ArrayList<Join>();
        final Join join = new Join();
        join.setInner(true);

        //is t a table of subselect?
        join.setRightItem(incrementalSingle(t));

        //EqualsTo lJoinOn = new EqualsTo();
        //lJoinOn.setLeftExpression();
        //lJoinOn.setRightExpression(new Column(join.getRightItem().getAlias().getName()
         //       +selectFrom.getJoins().get(0).getOnExpression().toString().substring(oldItem.toString().indexOf("."))));

        //join.setOnExpression(new );


        joinsL.add(join);
        lhs.setJoins(joinsL);
        unionParts.add(lhs);


        final PlainSelect rhs = getSimpleAllSelect(incrementalSingle(s));
        final List<Join> joinsR = new ArrayList<Join>();
        final Join join2 = new Join();
        join2.setInner(true);

        //is t a table of subselect?
        join2.setRightItem(mod(t));


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

    private FromItem mod(final FromItem r) {

        final SetOperationList unionList = new SetOperationList();
        final UnionOp union = new UnionOp();
        final List<PlainSelect> unionParts = new ArrayList<PlainSelect>();
        final List<SetOperation> setOps = new ArrayList<SetOperation>();
        setOps.add(union);
        unionParts.add(getSimpleAllSelect(r));
        unionParts.add(getSimpleAllSelect(incrementalSingle(r)));
        unionList.setOpsAndSelects(unionParts, setOps);
        SubSelect modQuery = new SubSelect();
        if (modQuery.getAlias() == null) {
            modQuery.setAlias(new Alias(generateRandomStr()));
        }
        modQuery.setSelectBody(unionList);

        return  modQuery;
    }

    private PlainSelect getSimpleAllSelect(FromItem fromItem) {
        final PlainSelect plainSelect = new PlainSelect();
        plainSelect.setFromItem(fromItem);
        plainSelect.setSelectItems(ALLSELECTITEMS);
        return plainSelect;
    }
}
