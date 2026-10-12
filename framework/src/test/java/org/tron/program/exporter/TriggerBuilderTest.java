package org.tron.program.exporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.Test;
import org.tron.common.logsfilter.trigger.InternalTransactionPojo;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.utils.StringUtil;
import org.tron.protos.Protocol.InternalTransaction;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.protos.contract.BalanceContract.DelegateResourceContract;
import org.tron.protos.contract.BalanceContract.TransferContract;
import org.tron.protos.contract.BalanceContract.UnDelegateResourceContract;
import org.tron.protos.contract.BalanceContract.WithdrawExpireUnfreezeContract;
import org.tron.protos.contract.SmartContractOuterClass.CreateSmartContract;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

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

  private static final byte[] OWNER = addr(0x11);
  private static final byte[] RECEIVER = addr(0x22);

  private static byte[] addr(int fill) {
    byte[] a = new byte[21];
    java.util.Arrays.fill(a, (byte) fill);
    a[0] = 0x41;
    return a;
  }

  private static String b58(byte[] a) {
    return StringUtil.encode58Check(a);
  }

  private static TransactionLogTrigger build(ContractType type, Message parameter,
      TransactionInfo info) {
    Transaction tx = Transaction.newBuilder().setRawData(Transaction.raw.newBuilder()
        .addContract(Transaction.Contract.newBuilder().setType(type)
            .setParameter(Any.pack(parameter)))).build();
    return TriggerBuilder.createTransactionLogTrigger(info, tx, "block", 1L, 2L, 0, null);
  }

  // Same fields as the live plugin (TransactionLogTriggerCapsule): deployments carry
  // the owner and the endowment as callValue.
  @Test
  public void createSmartContractCarriesOwnerAndCallValue() {
    TransactionLogTrigger t = build(ContractType.CreateSmartContract,
        CreateSmartContract.newBuilder().setOwnerAddress(ByteString.copyFrom(OWNER))
            .setNewContract(SmartContract.newBuilder().setCallValue(5L)).build(), null);
    assertEquals(b58(OWNER), t.getFromAddress());
    assertNull(t.getToAddress());
    assertEquals(5L, t.getContractCallValue());
  }

  @Test
  public void delegateAndUndelegateCarryReceiverAndBalance() {
    TransactionLogTrigger d = build(ContractType.DelegateResourceContract,
        DelegateResourceContract.newBuilder().setOwnerAddress(ByteString.copyFrom(OWNER))
            .setReceiverAddress(ByteString.copyFrom(RECEIVER)).setBalance(7L).build(), null);
    assertEquals(b58(OWNER), d.getFromAddress());
    assertEquals(b58(RECEIVER), d.getToAddress());
    assertEquals("trx", d.getAssetName());
    assertEquals(7L, d.getAssetAmount());

    TransactionLogTrigger u = build(ContractType.UnDelegateResourceContract,
        UnDelegateResourceContract.newBuilder().setOwnerAddress(ByteString.copyFrom(OWNER))
            .setReceiverAddress(ByteString.copyFrom(RECEIVER)).setBalance(8L).build(), null);
    assertEquals(b58(OWNER), u.getFromAddress());
    assertEquals(b58(RECEIVER), u.getToAddress());
    assertEquals(8L, u.getAssetAmount());
  }

  @Test
  public void withdrawExpireUnfreezeAmountComesFromTransactionInfo() {
    TransactionLogTrigger t = build(ContractType.WithdrawExpireUnfreezeContract,
        WithdrawExpireUnfreezeContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(OWNER)).build(),
        TransactionInfo.newBuilder().setWithdrawExpireAmount(9L).build());
    assertEquals(b58(OWNER), t.getFromAddress());
    assertEquals(9L, t.getAssetAmount());
  }

  // TransferContract's amount is assetAmount; callValue is 0 like the live plugin.
  @Test
  public void transferContractCallValueIsZero() {
    TransactionLogTrigger t = build(ContractType.TransferContract,
        TransferContract.newBuilder().setOwnerAddress(ByteString.copyFrom(OWNER))
            .setToAddress(ByteString.copyFrom(RECEIVER)).setAmount(3L).build(), null);
    assertEquals(0L, t.getContractCallValue());
    assertEquals(3L, t.getAssetAmount());
    assertEquals(b58(RECEIVER), t.getToAddress());
  }
}
