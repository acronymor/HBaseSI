package com.hbase.demo;

import com.hbase.demo.client.SidxOperation;
import com.hbase.demo.client.SidxResult;
import com.hbase.demo.client.SidxTable;
import com.hbase.demo.condition.*;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;

/**
 * @author apktool
 * @title com.hbase.demo.GetDemo
 * @description TODO
 * @date 2019-10-06 11:47
 */
@Service
public class GetDemo {

    @Setter(onMethod = @__({@Autowired}))
    private SidxOperation sidxOperation;

    public void start(String[] args) {
        SidxTable table = new SidxTable().of("test");
        queryBySingleCondition(table);
        queryByMultiCondition(table);
    }

    private void queryBySingleCondition(SidxTable table) {
        /* c1:f2 = 1L */
        Long value = 1L;

        SidxIdentifier identifier = new SidxIdentifier(Bytes.toBytes("f2"), Bytes.toBytes("c1"));

        SidxLiteral literal = new SidxLiteral(Bytes.toBytes(value));

        AbstractSidxNode[] operators = new AbstractSidxNode[2];
        operators[0] = identifier;
        operators[1] = literal;

        SidxOperator operator = new SidxOperator("EQUAL", SidxOperator.SidxKind.EQUAL);

        SidxCall call = new SidxCall(operator, operators);

        SidxResult sidxResult = sidxOperation.getSync(table, call);
        Iterator<Result> iterator = sidxResult.getIterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            System.out.println(new String(result.getRow()));
            byte[] data = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
            System.out.println(Bytes.toInt(data));
        }
    }

    private void queryByMultiCondition(SidxTable table) {
        /* f1:c1 >= 3 and f2:c1 > 5 OR f2:c1 != 8*/
        Long value3 = 8L;
        SidxLiteral literal3 = new SidxLiteral(Bytes.toBytes(value3));
        SidxIdentifier identifier3 = new SidxIdentifier(Bytes.toBytes("f2"), Bytes.toBytes("c1"));
        AbstractSidxNode[] operators3 = new AbstractSidxNode[2];
        operators3[0] = identifier3;
        operators3[1] = literal3;
        SidxOperator operator3 = new SidxOperator("NOT_EQUAL", SidxOperator.SidxKind.NOT_EQUAL);
        SidxCall call3 = new SidxCall(operator3, operators3);


        Long value2 = 5L;
        SidxLiteral literal2 = new SidxLiteral(Bytes.toBytes(value2));
        SidxIdentifier identifier2 = new SidxIdentifier(Bytes.toBytes("f2"), Bytes.toBytes("c1"));
        AbstractSidxNode[] operators2 = new AbstractSidxNode[2];
        operators2[0] = identifier2;
        operators2[1] = literal2;
        SidxOperator operator2 = new SidxOperator("GREATER", SidxOperator.SidxKind.GREATER);
        SidxCall call2 = new SidxCall(operator2, operators2);


        Integer value1 = 3;
        SidxLiteral literal1 = new SidxLiteral(Bytes.toBytes(value1));
        SidxIdentifier identifier1 = new SidxIdentifier(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
        AbstractSidxNode[] operators1 = new AbstractSidxNode[2];
        operators1[0] = identifier1;
        operators1[1] = literal1;
        SidxOperator operator1 = new SidxOperator("GREATER_OR_EQUAL", SidxOperator.SidxKind.GREATER_OR_EQUAL);
        SidxCall call1 = new SidxCall(operator1, operators1);


        SidxOperator operator12 = new SidxOperator("AND", SidxOperator.SidxKind.AND);
        AbstractSidxNode[] operators12 = new AbstractSidxNode[2];
        operators12[0] = call1;
        operators12[1] = call2;
        SidxCall call12 = new SidxCall(operator12, operators12);

        SidxOperator operator23 = new SidxOperator("OR", SidxOperator.SidxKind.OR);
        AbstractSidxNode[] operators23 = new AbstractSidxNode[2];
        operators23[0] = call12;
        operators23[1] = call3;
        SidxCall call = new SidxCall(operator23, operators23);

        SidxResult result = query(table, call, new Pair<>());
        System.out.println("---------------------------------");
        result.getIterator().forEachRemaining(t -> System.out.println(Bytes.toString(t.getRow())));
    }


    private SidxResult query(SidxTable table, SidxCall call, Pair<SidxResult, SidxResult> pair) {

        AbstractSidxNode[] operators = call.getOperators();

        for (AbstractSidxNode node : operators) {
            if (node instanceof SidxCall) {
                AbstractSidxNode left = ((SidxCall) node).getOperators()[0];
                AbstractSidxNode right = ((SidxCall) node).getOperators()[1];

                if (!(left instanceof SidxCall) && !(right instanceof SidxCall)) {
                    SidxResult result = sidxOperation.getSync(table, (SidxCall) node);
                    if (pair.getSecond() == null) {
                        pair.setSecond(result);
                    } else {
                        pair.setFirst(result);
                    }

                } else {
                    query(table, (SidxCall) node, pair);
                }
            }
        }

        SidxResult left = pair.getFirst();
        SidxResult right = pair.getSecond();

        if (call.getOperator().getKind().equals(SidxOperator.SidxKind.AND)) {
            left = left.retainAll(right);
        }

        if (call.getOperator().getKind().equals(SidxOperator.SidxKind.OR)) {
            left = left.addAll(right);
        }

        pair.setSecond(null);

        return left;
    }


    private void test(SidxCall call) {

        AbstractSidxNode[] operators = call.getOperators();

        for (AbstractSidxNode node : operators) {
            if (node instanceof SidxCall) {
                AbstractSidxNode left = ((SidxCall) node).getOperators()[0];
                AbstractSidxNode right = ((SidxCall) node).getOperators()[1];

                if (!(left instanceof SidxCall) && !(right instanceof SidxCall)) {
                    printSidxCall((SidxCall) node);
                } else {
                    test((SidxCall) node);
                }
            }
        }
        System.out.println(call.getOperator().getKind());
    }

    private void printSidxCall(SidxCall call) {

        SidxIdentifier identifier = (SidxIdentifier) call.getOperators()[0];
        SidxLiteral literal = (SidxLiteral) call.getOperators()[1];
        SidxOperator.SidxKind kind = call.getOperator().getKind();

        String f1c1 = "f1:c1";
        String f2c1 = "f2:c1";

        String cfv = Bytes.toString(identifier.getFamilyIdentifier()) + ":" + Bytes.toString(identifier.getColumnIdentifier());

        if (f1c1.equals(cfv)) {
            System.out.println(cfv + " " + kind + " " + Bytes.toInt(literal.getLiteral()));
        }

        if (f2c1.equals(cfv)) {
            System.out.println(cfv + " " + kind + " " + Bytes.toLong(literal.getLiteral()));
        }
    }
}
