package org.tron.common.logsfilter;

import static org.tron.core.config.Parameter.ChainConstant.TRX_PRECISION;
import static org.tron.protos.contract.Common.ResourceCode.BANDWIDTH;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tron.common.logsfilter.capsule.TransactionLogTriggerCapsule;
import org.tron.common.utils.Sha256Hash;
import org.tron.common.utils.StringUtil;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.p2p.utils.ByteArray;
import org.tron.protos.Protocol;
import org.tron.protos.contract.AssetIssueContractOuterClass;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.Common;
import org.tron.protos.contract.SmartContractOuterClass;

public class TransactionLogTriggerCapsuleTest {

  private static final String OWNER_ADDRESS = "41548794500882809695a8a687866e76d4271a1abc";
  private static final String RECEIVER_ADDRESS = "41abd4b9367799eaa3197fecb144eb71de1e049150";
  private static final String CONTRACT_ADDRESS = "A0B4750E2CD76E19DCA331BF5D089B71C3C2798548";

  public TransactionCapsule transactionCapsule;
  public BlockCapsule blockCapsule;

  @Before
  public void setup() {
    blockCapsule = new BlockCapsule(1, Sha256Hash.ZERO_HASH,
        System.currentTimeMillis(), Sha256Hash.ZERO_HASH.getByteString());
  }

  @Test
  public void testConstructorWithUnfreezeBalanceTrxCapsule() {
    BalanceContract.UnfreezeBalanceContract.Builder builder2 =
        BalanceContract.UnfreezeBalanceContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setReceiverAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)));
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.UnfreezeBalanceContract);
    Protocol.TransactionInfo.Builder builder = Protocol.TransactionInfo.newBuilder();
    builder.setUnfreezeAmount(TRX_PRECISION + 1000);


    TransactionLogTriggerCapsule triggerCapsule = new TransactionLogTriggerCapsule(
        transactionCapsule, blockCapsule,0,0,0,
        builder.build(),0);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getToAddress());
    Assert.assertEquals(TRX_PRECISION + 1000,
        triggerCapsule.getTransactionLogTrigger().getAssetAmount());
  }


  @Test
  public void testConstructorWithFreezeBalanceV2TrxCapsule() {
    BalanceContract.FreezeBalanceV2Contract.Builder builder2 =
        BalanceContract.FreezeBalanceV2Contract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setFrozenBalance(TRX_PRECISION + 100000)
        .setResource(Common.ResourceCode.BANDWIDTH);
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.FreezeBalanceV2Contract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertEquals("trx", triggerCapsule.getTransactionLogTrigger().getAssetName());
    Assert.assertEquals(TRX_PRECISION + 100000,
        triggerCapsule.getTransactionLogTrigger().getAssetAmount());
  }

  @Test
  public void testConstructorWithUnfreezeBalanceV2TrxCapsule() {
    BalanceContract.UnfreezeBalanceV2Contract.Builder builder2 =
        BalanceContract.UnfreezeBalanceV2Contract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setUnfreezeBalance(TRX_PRECISION + 4000)
        .setResource(Common.ResourceCode.BANDWIDTH);
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.UnfreezeBalanceV2Contract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertEquals("trx", triggerCapsule.getTransactionLogTrigger().getAssetName());
    Assert.assertEquals(TRX_PRECISION + 4000,
        triggerCapsule.getTransactionLogTrigger().getAssetAmount());
  }


  @Test
  public void testConstructorWithWithdrawExpireTrxCapsule() {
    BalanceContract.WithdrawExpireUnfreezeContract.Builder builder2 =
        BalanceContract.WithdrawExpireUnfreezeContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)));
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.WithdrawExpireUnfreezeContract);

    Protocol.TransactionInfo.Builder builder = Protocol.TransactionInfo.newBuilder();
    builder.setWithdrawExpireAmount(TRX_PRECISION + 1000);

    TransactionLogTriggerCapsule triggerCapsule = new TransactionLogTriggerCapsule(
        transactionCapsule, blockCapsule,0,0,0,
        builder.build(),0);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertEquals("trx", triggerCapsule.getTransactionLogTrigger().getAssetName());
    Assert.assertEquals(TRX_PRECISION + 1000,
        triggerCapsule.getTransactionLogTrigger().getAssetAmount());
  }


  @Test
  public void testConstructorWithDelegateResourceTrxCapsule() {
    BalanceContract.DelegateResourceContract.Builder builder2 =
        BalanceContract.DelegateResourceContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setReceiverAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)))
        .setBalance(TRX_PRECISION + 2000);
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.DelegateResourceContract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getToAddress());
    Assert.assertEquals("trx", triggerCapsule.getTransactionLogTrigger().getAssetName());
    Assert.assertEquals(TRX_PRECISION + 2000,
        triggerCapsule.getTransactionLogTrigger().getAssetAmount());
  }

  @Test
  public void testConstructorWithUnDelegateResourceTrxCapsule() {
    BalanceContract.UnDelegateResourceContract.Builder builder2 =
        BalanceContract.UnDelegateResourceContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setReceiverAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)))
        .setBalance(TRX_PRECISION + 10000);
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.UnDelegateResourceContract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getToAddress());
    Assert.assertEquals("trx", triggerCapsule.getTransactionLogTrigger().getAssetName());
    Assert.assertEquals(TRX_PRECISION + 10000,
        triggerCapsule.getTransactionLogTrigger().getAssetAmount());
  }

  @Test
  public void testConstructorWithCancelAllUnfreezeTrxCapsule() {
    BalanceContract.CancelAllUnfreezeV2Contract.Builder builder2 =
        BalanceContract.CancelAllUnfreezeV2Contract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)));
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.CancelAllUnfreezeV2Contract);

    Protocol.TransactionInfo.Builder builder = Protocol.TransactionInfo.newBuilder();
    builder.clearCancelUnfreezeV2Amount().putCancelUnfreezeV2Amount(
        BANDWIDTH.name(), TRX_PRECISION + 2000);

    TransactionLogTriggerCapsule triggerCapsule = new TransactionLogTriggerCapsule(
        transactionCapsule, blockCapsule,0,0,0,
        builder.build(),0);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertEquals(TRX_PRECISION + 2000,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get(BANDWIDTH.name()).longValue());
  }


  @Test
  public void testConstructorWithTransferCapsule() {
    BalanceContract.TransferContract.Builder builder2 =
        BalanceContract.TransferContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
            .setToAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)));
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.TransferContract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getToAddress());
  }

  @Test
  public void testConstructorWithTransferAssetCapsule() {
    AssetIssueContractOuterClass.TransferAssetContract.Builder builder2 =
        AssetIssueContractOuterClass.TransferAssetContract.newBuilder()
            .setAssetName(ByteString.copyFrom("AssetName".getBytes()))
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
            .setToAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)));
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.TransferAssetContract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getToAddress());
  }

  @Test
  public void testTransactionMetadataInitializedForPluginTrigger() {
    BalanceContract.TransferContract contract = BalanceContract.TransferContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setToAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)))
        .setAmount(TRX_PRECISION + 1)
        .build();
    transactionCapsule = createTransactionCapsule(contract,
        Protocol.Transaction.Contract.ContractType.TransferContract,
        101L, 202L, 303L);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getTransactionDetail());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getExtMap());
    Assert.assertEquals(101L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("refBlockNum").longValue());
    Assert.assertEquals(202L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("expiration").longValue());
    Assert.assertEquals(303L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("timestamp").longValue());
  }

  @Test
  public void testConstructorWithTriggerSmartContract() {
    SmartContractOuterClass.TriggerSmartContract.Builder builder2 =
        SmartContractOuterClass.TriggerSmartContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
            .setContractAddress(ByteString.copyFrom(ByteArray.fromHexString(CONTRACT_ADDRESS)));
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.TriggerSmartContract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getToAddress());
  }

  @Test
  public void testConstructorWithCreateSmartContract() {
    SmartContractOuterClass.CreateSmartContract.Builder builder2 =
        SmartContractOuterClass.CreateSmartContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)));
    transactionCapsule = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.CreateSmartContract);

    TransactionLogTriggerCapsule triggerCapsule =
        new TransactionLogTriggerCapsule(transactionCapsule, blockCapsule);

    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getFromAddress());
  }

  @Test
  public void testConstructorWithCreateTransactionInfo() {
    Protocol.TransactionInfo.Builder infoBuild = Protocol.TransactionInfo.newBuilder();

    Protocol.ResourceReceipt.Builder resourceBuild = Protocol.ResourceReceipt.newBuilder();
    resourceBuild.setEnergyFee(1);
    resourceBuild.setEnergyUsageTotal(2);
    resourceBuild.setEnergyUsage(3);
    resourceBuild.setOriginEnergyUsage(4);
    resourceBuild.setNetFee(5);
    resourceBuild.setNetUsage(6);

    infoBuild
        .setContractAddress(ByteString.copyFrom(ByteArray.fromHexString(CONTRACT_ADDRESS)))
        .addContractResult(ByteString.copyFrom(ByteArray.fromHexString("112233")))
        .setReceipt(resourceBuild.build());

    SmartContractOuterClass.CreateSmartContract.Builder builder2 =
        SmartContractOuterClass.CreateSmartContract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)));
    TransactionCapsule tc = new TransactionCapsule(builder2.build(),
        Protocol.Transaction.Contract.ContractType.CreateSmartContract);

    BlockCapsule bc = new BlockCapsule(1, Sha256Hash.ZERO_HASH,
        System.currentTimeMillis(), Sha256Hash.ZERO_HASH.getByteString());


    TransactionLogTriggerCapsule trigger =
        new TransactionLogTriggerCapsule(tc, bc, infoBuild.build());

    Assert.assertEquals(1, trigger.getTransactionLogTrigger().getEnergyFee());
    Assert.assertEquals(2, trigger.getTransactionLogTrigger().getEnergyUsageTotal());
    Assert.assertEquals(3, trigger.getTransactionLogTrigger().getEnergyUsage());
    Assert.assertEquals(4, trigger.getTransactionLogTrigger().getOriginEnergyUsage());
    Assert.assertEquals(5, trigger.getTransactionLogTrigger().getNetFee());
    Assert.assertEquals(6, trigger.getTransactionLogTrigger().getNetUsage());

    Assert.assertEquals(StringUtil.encode58Check(Hex.decode(CONTRACT_ADDRESS)),
        trigger.getTransactionLogTrigger().getContractAddress());
    Assert.assertEquals("112233", trigger.getTransactionLogTrigger().getContractResult());
  }

  @Test
  public void testExtMapMergesTransactionInfoFields() {
    BalanceContract.CancelAllUnfreezeV2Contract contract =
        BalanceContract.CancelAllUnfreezeV2Contract.newBuilder()
            .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
            .build();
    transactionCapsule = createTransactionCapsule(contract,
        Protocol.Transaction.Contract.ContractType.CancelAllUnfreezeV2Contract,
        11L, 22L, 33L);

    Protocol.TransactionInfo transactionInfo = Protocol.TransactionInfo.newBuilder()
        .putCancelUnfreezeV2Amount(BANDWIDTH.name(), TRX_PRECISION + 2000)
        .setWithdrawExpireAmount(44L)
        .setPackingFee(55L)
        .build();

    TransactionLogTriggerCapsule triggerCapsule = new TransactionLogTriggerCapsule(
        transactionCapsule, blockCapsule, 0, 0, 0, transactionInfo, 0);

    Assert.assertEquals(11L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("refBlockNum").longValue());
    Assert.assertEquals(22L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("expiration").longValue());
    Assert.assertEquals(33L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("timestamp").longValue());
    Assert.assertEquals(TRX_PRECISION + 2000,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get(BANDWIDTH.name()).longValue());
    Assert.assertEquals(44L,
        triggerCapsule.getTransactionLogTrigger().getExtMap()
            .get("withdrawExpireAmount").longValue());
    Assert.assertEquals(55L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("packingFee").longValue());
  }

  @Test
  public void testTransactionInfoFallbackFieldsInitialized() {
    BalanceContract.TransferContract contract = BalanceContract.TransferContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setToAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)))
        .setAmount(TRX_PRECISION + 9)
        .build();
    transactionCapsule = createTransactionCapsule(contract,
        Protocol.Transaction.Contract.ContractType.TransferContract,
        9L, 10L, 11L,
        ByteString.copyFrom("memo".getBytes()),
        Arrays.asList(ByteString.copyFrom("sig-a".getBytes()),
            ByteString.copyFrom("sig-b".getBytes())));

    Protocol.InternalTransaction internalTransaction = Protocol.InternalTransaction.newBuilder()
        .setHash(ByteString.copyFrom(ByteArray.fromHexString("112233")))
        .setCallerAddress(ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)))
        .setTransferToAddress(ByteString.copyFrom(ByteArray.fromHexString(RECEIVER_ADDRESS)))
        .addCallValueInfo(Protocol.InternalTransaction.CallValueInfo.newBuilder()
            .setCallValue(66L)
            .build())
        .addCallValueInfo(Protocol.InternalTransaction.CallValueInfo.newBuilder()
            .setTokenId("1002001")
            .setCallValue(77L)
            .build())
        .setNote(ByteString.copyFrom("note".getBytes()))
        .setExtra("extra")
        .setRejected(true)
        .build();

    Protocol.TransactionInfo transactionInfo = Protocol.TransactionInfo.newBuilder()
        .setFee(999L)
        .setResult(Protocol.TransactionInfo.code.FAILED)
        .setAssetIssueID("123456")
        .setWithdrawAmount(11L)
        .setUnfreezeAmount(12L)
        .setExchangeId(13L)
        .setExchangeReceivedAmount(14L)
        .setExchangeInjectAnotherAmount(15L)
        .setExchangeWithdrawAnotherAmount(16L)
        .setShieldedTransactionFee(17L)
        .setPackingFee(18L)
        .setReceipt(Protocol.ResourceReceipt.newBuilder()
            .setResult(Protocol.Transaction.Result.contractResult.REVERT)
            .build())
        .addInternalTransactions(internalTransaction)
        .build();

    TransactionLogTriggerCapsule triggerCapsule = new TransactionLogTriggerCapsule(
        transactionCapsule, blockCapsule, transactionInfo);

    Assert.assertEquals(999L, triggerCapsule.getTransactionLogTrigger().getFee());
    Assert.assertEquals("REVERT", triggerCapsule.getTransactionLogTrigger().getResult());
    Assert.assertTrue(triggerCapsule.getTransactionLogTrigger().getTxResult().contains("fee: 999"));
    Assert.assertTrue(triggerCapsule.getTransactionLogTrigger().getTxResult()
        .contains("ret: FAILED"));
    Assert.assertTrue(triggerCapsule.getTransactionLogTrigger().getTxResult()
        .contains("contractRet: REVERT"));
    Assert.assertEquals(1_000_000L, triggerCapsule.getTransactionLogTrigger().getMemoFee());
    Assert.assertEquals(1_000_000L, triggerCapsule.getTransactionLogTrigger().getMultiSignFee());
    Assert.assertNotNull(triggerCapsule.getTransactionLogTrigger().getInternalTransactionList());
    Assert.assertEquals(1,
        triggerCapsule.getTransactionLogTrigger().getInternalTransactionList().size());
    Assert.assertEquals(66L, triggerCapsule.getTransactionLogTrigger().getInternalTransactionList()
        .get(0).getCallValue());
    Assert.assertEquals(77L, triggerCapsule.getTransactionLogTrigger().getInternalTransactionList()
        .get(0).getTokenInfo().get("1002001").longValue());
    Assert.assertEquals("note", triggerCapsule.getTransactionLogTrigger()
        .getInternalTransactionList().get(0).getNote());
    Assert.assertEquals("extra", triggerCapsule.getTransactionLogTrigger()
        .getInternalTransactionList().get(0).getExtra());
    Assert.assertEquals(123456L,
        triggerCapsule.getTransactionLogTrigger().getExtMap().get("assetIssueID").longValue());
  }

  private TransactionCapsule createTransactionCapsule(com.google.protobuf.Message contractMessage,
      Protocol.Transaction.Contract.ContractType contractType,
      long refBlockNum, long expiration, long timestamp) {
    return createTransactionCapsule(contractMessage, contractType, refBlockNum, expiration,
        timestamp, ByteString.EMPTY, null);
  }

  private TransactionCapsule createTransactionCapsule(com.google.protobuf.Message contractMessage,
      Protocol.Transaction.Contract.ContractType contractType,
      long refBlockNum, long expiration, long timestamp,
      ByteString data, java.util.List<ByteString> signatures) {
    Protocol.Transaction.raw rawData = Protocol.Transaction.raw.newBuilder()
        .addContract(Protocol.Transaction.Contract.newBuilder()
            .setType(contractType)
            .setParameter(Any.pack(contractMessage))
            .build())
        .setRefBlockNum(refBlockNum)
        .setExpiration(expiration)
        .setTimestamp(timestamp)
        .setData(data)
        .build();

    Protocol.Transaction.Builder transactionBuilder = Protocol.Transaction.newBuilder()
        .setRawData(rawData);
    if (signatures != null) {
      transactionBuilder.addAllSignature(signatures);
    }
    return new TransactionCapsule(transactionBuilder.build());
  }

}
