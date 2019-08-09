package org.tron.stresstest.dispatch.creator.contract;

import static org.tron.core.Wallet.addressValid;

import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.spongycastle.util.encoders.Hex;
import org.tron.common.crypto.ECKey;
import org.tron.common.crypto.ECKey.ECDSASignature;
import org.tron.common.utils.Base58;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.Wallet;
import org.tron.protos.Contract.TriggerSmartContract;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.stresstest.AbiUtil;
import org.tron.stresstest.dispatch.AbstractTransactionCreator;
import org.tron.stresstest.dispatch.GoodCaseTransactonCreator;
import org.tron.stresstest.dispatch.TransactionFactory;
import org.tron.stresstest.dispatch.creator.CreatorCounter;
import org.tron.stresstest.exception.EncodingException;


@Setter
public class WithdrawCreator extends AbstractTransactionCreator implements
    GoodCaseTransactonCreator {

  private String ownerAddress = WithdrawToAddress;
  private String contractAddress = SideGatewayContractAddress;
  private long callValue = 1L;
  private String methodSign = "withdrawTRX()";
  private boolean hex = false;
  private String param = "";
  private long feeLimit = 1000000000L;
  private String privateKey = WithdrawToPrivateKey;
  public static AtomicLong queryCount = new AtomicLong();
  private List<String> ownerAddressList = new CopyOnWriteArrayList<>();


  {
    try {
      File filename = new File("/Users/tron/git/java-tron/accounts.txt");
      InputStreamReader reader = new InputStreamReader(
          new FileInputStream(filename)); // 建立一个输入流对象reader
      BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
      String line = "";
      line = br.readLine();
      while (line != null) {
        ownerAddressList.add(line);
        line = br.readLine();
      }
      br.close();
      reader.close();
    } catch (Exception e) {

    }

  }

  @Override
  protected Protocol.Transaction create() {
//    queryCount.incrementAndGet();

    int id = Long.valueOf(queryCount.incrementAndGet()).intValue();
    int index = id % ownerAddressList.size();
    String[] addressAndPri = ownerAddressList.get(index).split(",");
    if (addressAndPri == null || addressAndPri.length != 2) {
      return null;
    }
    byte[] ownerAddressBytes = Wallet.decodeFromBase58Check(addressAndPri[0]);
    //byte[] ownerAddressBytes = Wallet.decodeFromBase58Check(ownerAddress);

    TransactionFactory.context.getBean(CreatorCounter.class).put(this.getClass().getName());

    TriggerSmartContract contract = null;
    try {
      contract = triggerCallContract(
          ownerAddressBytes,
          Wallet.decodeFromBase58Check(contractAddress),
//              contractAddress.getBytes(),
          callValue,
          Hex.decode(AbiUtil.parseMethod(
              methodSign,
              param,
              hex
          )));
    } catch (EncodingException e) {
      e.printStackTrace();
    }

    Protocol.Transaction transaction = createTransaction(contract,
        ContractType.TriggerSmartContract);

    transaction = transaction.toBuilder()
        .setRawData(transaction.getRawData().toBuilder().setFeeLimit(feeLimit).build()).build();
    String mainGateWay = "TUmGh8c2VcpfmJ7rBYq1FU9hneXhz3P8z3";
    transaction = sign(transaction, ECKey.fromPrivate(ByteArray.fromHexString(privateKey)),
        decodeFromBase58Check(mainGateWay), false);
    //transaction = sign(transaction, ECKey.fromPrivate(ByteArray.fromHexString(privateKey)));
    return transaction;
  }

  public static byte[] decodeFromBase58Check(String addressBase58) {
    if (StringUtils.isEmpty(addressBase58)) {
      return null;
    }
    byte[] address = decode58Check(addressBase58);
    if (!addressValid(address)) {
      return null;
    }
    return address;
  }

  private static byte[] decode58Check(String input) {
    byte[] decodeCheck = Base58.decode(input);
    if (decodeCheck.length <= 4) {
      return null;
    }
    byte[] decodeData = new byte[decodeCheck.length - 4];
    System.arraycopy(decodeCheck, 0, decodeData, 0, decodeData.length);
    byte[] hash0 = Sha256Hash.hash(decodeData);
    byte[] hash1 = Sha256Hash.hash(hash0);
    if (hash1[0] == decodeCheck[decodeData.length]
        && hash1[1] == decodeCheck[decodeData.length + 1]
        && hash1[2] == decodeCheck[decodeData.length + 2]
        && hash1[3] == decodeCheck[decodeData.length + 3]) {
      return decodeData;
    }
    return null;
  }

  /**
   * constructor.
   */
  public static Transaction sign(Transaction transaction, ECKey myKey, byte[] chainId,
      boolean isMainChain) {
    Transaction.Builder transactionBuilderSigned = transaction.toBuilder();
    byte[] hash = Sha256Hash.hash(transaction.getRawData().toByteArray());

    byte[] newHash;
    if (isMainChain) {
      newHash = hash;
    } else {
      byte[] hashWithChainId = Arrays.copyOf(hash, hash.length + chainId.length);
      System.arraycopy(chainId, 0, hashWithChainId, hash.length, chainId.length);
      newHash = Sha256Hash.hash(hashWithChainId);
    }

    ECDSASignature signature = myKey.sign(newHash);
    ByteString bsSign = ByteString.copyFrom(signature.toByteArray());
    transactionBuilderSigned.addSignature(bsSign);
    transaction = transactionBuilderSigned.build();
    return transaction;
  }

}