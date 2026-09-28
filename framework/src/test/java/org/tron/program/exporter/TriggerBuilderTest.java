package org.tron.program.exporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.protobuf.ByteString;
import org.junit.Test;
import org.tron.common.logsfilter.trigger.InternalTransactionPojo;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.protos.Protocol.InternalTransaction;
import org.tron.protos.Protocol.TransactionInfo;

public class TriggerBuilderTest {

  @Test
  public void shouldMapInternalTransactionExtraAndAllCallValueInfo() {
    InternalTransaction internalTransaction = InternalTransaction.newBuilder()
        .setHash(ByteString.copyFromUtf8("hash"))
        .setCallerAddress(ByteString.copyFrom(new byte[21]))
        .setTransferToAddress(ByteString.copyFrom(new byte[21]))
        .addCallValueInfo(InternalTransaction.CallValueInfo.newBuilder()
            .setCallValue(66L))
        .addCallValueInfo(InternalTransaction.CallValueInfo.newBuilder()
            .setTokenId("1002001")
            .setCallValue(77L))
        .setExtra("{\"vote\":true}")
        .build();
    TransactionInfo transactionInfo = TransactionInfo.newBuilder()
        .addInternalTransactions(internalTransaction)
        .build();

    TransactionLogTrigger trigger = TriggerBuilder.createTransactionLogTrigger(
        transactionInfo, null, "block", 1L, 2L, 0, null);
    InternalTransactionPojo pojo = trigger.getInternalTransactionList().get(0);

    assertEquals(66L, pojo.getCallValue());
    assertEquals(Long.valueOf(77L), pojo.getTokenInfo().get("1002001"));
    assertEquals("{\"vote\":true}", pojo.getExtra());
    assertNull(pojo.getData());
  }
}
