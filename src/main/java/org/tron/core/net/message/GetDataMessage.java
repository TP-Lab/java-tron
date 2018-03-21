package org.tron.core.net.message;

import org.tron.common.overlay.message.Message;

public class GetDataMessage extends Message {

  public GetDataMessage() {
    super();
  }


  public GetDataMessage(byte[] packed) {
    super(packed);
  }

  @Override
  public byte[] getData() {
    return new byte[0];
  }

  @Override
  public String toString() {
    return null;
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

  @Override
  public MessageTypes getType() {
    return null;
  }
}
