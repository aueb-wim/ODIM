import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) throws JSQLParserException {
        String query = "select S1.p_num, S3.e_num, S2.exam, S2.value\n" +
                "from S1, S3, S2\n" +
                "where (S1.p_ide = S3.p_ide) and S3.e_ide = S2.e_ide";

        String normQuery = "select X.p_num, X.e_num, X.exam, X.value\n" +
                "from (select *\n" +
                "      from (select *\n" +
                "            from S1\n" +
                "      \t    INNER JOIN S3\n" +
                "            on S3.p_ide = S1.p_ide) as Y\n" +
                "      INNER JOIN S2\n" +
                "      on Y.e_ide = S2.e_ide) as X\n";
        List<String> incrementalSet = new ArrayList<String>();
        incrementalSet.add("S1");
        incrementalSet.add("S2");
        incrementalSet.add("S3");

        RefactoredTransformer t = new RefactoredTransformer(incrementalSet);

        //TEST PROJECTION.
        //Input "select X.A, X.B from X"
        //output: select X.A, X.B from X
        //PlainSelect testProjection = (PlainSelect)((Select) CCJSqlParserUtil.parse("select A, B from S1")).getSelectBody(); // Explicit select not working...
        //System.out.println(t.incrementalProjection(testProjection));
        //System.out.println(t.transformQuery(normQuery));


        System.out.println("TEST STARTS HERE-------");

        try {
            Select r = t.incrementalProjection( (PlainSelect)(((Select)(CCJSqlParserUtil.parse("select id, test from S1"))).getSelectBody()));
            System.out.println(r);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

    }
}