package streamnative.transaction.test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionTest {
    public static void main(String[] args) throws Throwable{
        TransactionTimeoutChaosTest transactionTimeoutChaosTest = new TransactionTimeoutChaosTest(args[0], args[1]);
        Thread.sleep(1000000000);
    }
}
