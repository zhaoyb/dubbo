/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.dubbo;

import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.encodeInvocationArgument;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DECODE_IN_IO_THREAD_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DEFAULT_DECODE_IN_IO_THREAD;

import java.io.IOException;
import java.io.InputStream;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.codec.ExchangeCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;

/**
 * dubbo 编码，解码器
 *
 * 这里注意， dubbo协议， 是一个协议， 而不是序列化， 序列化是hessoin ,gson  json
 *
 * 整个dubbo 的编码 解码体系可以这样理解
 *
 *  dubbo协议， 其实是抽象于 hession, gson  json 之上
 *  而所谓的对dubbo做编码，其实就是对dubbo对象做序列化，而这个序列化的方式，就是hession或者gson底层具体的序列化协议
 *
 *  在这个序列化协议之下，
 *
 *  还有netty 网络层的编码和解码协议。
 *
 *
 * 另外， 注意在解码部分，
 *      作为服务端， 客户端的请求被解析成 DecodeableRpcInvocation 对象
 *      作为客户端， 服务端返回的结果被序列化为 DecodeableRpcResult 对象
 *
 *
 * 补充： 之前dubbo是带序列化方法的，不过目前应被移除了
 *
 *
 *
 * Dubbo codec.
 */
public class DubboCodec extends ExchangeCodec {

    /**
     * 协议名称
     */
    public static final String NAME = "dubbo";
    /**
     * 协议版本
     */
    public static final String DUBBO_VERSION = Version.getProtocolVersion();

    //------------------------下面几个是状态

    /**
     * 响应异常
     */
    public static final byte RESPONSE_WITH_EXCEPTION = 0;
    /**
     * 响应正常
     *
     */
    public static final byte RESPONSE_VALUE = 1;
    /**
     * 响应空
     *
     */
    public static final byte RESPONSE_NULL_VALUE = 2;

    /**
     * 响应异常，切带附件
     *
     */
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;

    /**
     * 响应正常，切带附件
     *
     */
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;

    /**
     * 响应空值，切带附件
     *
     */
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;

    /**
     * 方法参数空
     *
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /**
     * 方法参数空 类型
     *
     */
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];
    private static final Logger log = LoggerFactory.getLogger(DubboCodec.class);

    /**
     *
     *  解码body
     * @param channel
     * @param is
     * @param header
     * @return
     * @throws IOException
     */
    @Override
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        byte flag = header[2];
        // 解码协议
        byte proto = (byte) (flag & SERIALIZATION_MASK);
        // get request id.
        // request id,请求id
        long id = Bytes.bytes2long(header, 4);
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response. 解码响应
            Response res = new Response(id);
            // 是否是事件
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(true);
            }
            // get status.
            byte status = header[3];
            res.setStatus(status);
            try {
                if (status == Response.OK) {
                    Object data;
                    if (res.isEvent()) {
                        // 事件解析
                        ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                        data = decodeEventData(channel, in);
                    } else {
                        DecodeableRpcResult result;
                        // 在IO线程中解析
                        if (channel.getUrl().getParameter(DECODE_IN_IO_THREAD_KEY, DEFAULT_DECODE_IN_IO_THREAD)) {
                            result = new DecodeableRpcResult(channel, res, is,
                                                             (Invocation) getRequestData(id), proto);
                            result.decode();
                        } else {
                            // 正常解析
                            result = new DecodeableRpcResult(channel, res,
                                                             new UnsafeByteArrayInputStream(readMessageData(is)),
                                                             (Invocation) getRequestData(id), proto);
                        }
                        data = result;
                    }
                    res.setResult(data);
                } else {
                    // 异常状态
                    ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode response failed: " + t.getMessage(), t);
                }
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // decode request.   解码请求
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion());
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);

            // 是否是事件
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(true);
            }
            try {
                Object data;
                if (req.isEvent()) {
                    // 如果是事件，则解析事件
                    ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                    data = decodeEventData(channel, in);
                } else {
                    // 普通请求
                    DecodeableRpcInvocation inv;
                    // 是否在IO线程解码，默认false
                    if (channel.getUrl().getParameter(DECODE_IN_IO_THREAD_KEY, DEFAULT_DECODE_IN_IO_THREAD)) {
                        inv = new DecodeableRpcInvocation(channel, req, is, proto);
                        inv.decode();
                    } else {
                        inv = new DecodeableRpcInvocation(channel, req,
                                                          new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                    }
                    data = inv;
                }
                // 设置结果
                req.setData(data);
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode request failed: " + t.getMessage(), t);
                }
                // bad request
                req.setBroken(true);
                req.setData(t);
            }

            return req;
        }
    }

    private byte[] readMessageData(InputStream is) throws IOException {
        if (is.available() > 0) {
            byte[] result = new byte[is.available()];
            is.read(result);
            return result;
        }
        return new byte[]{};
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(channel, out, data, DUBBO_VERSION);
    }

    /**
     * 编码 请求体
     *
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        RpcInvocation inv = (RpcInvocation) data;

        // 写入dubbo版本
        out.writeUTF(version);
        // path
        out.writeUTF(inv.getAttachment(PATH_KEY));
        // version
        out.writeUTF(inv.getAttachment(VERSION_KEY));

        // 方法名
        out.writeUTF(inv.getMethodName());
        // 参数类型
        out.writeUTF(inv.getParameterTypesDesc());
        // 参数
        Object[] args = inv.getArguments();
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                out.writeObject(encodeInvocationArgument(channel, inv, i));
            }
        }
        // 附件，隐式传参，用于上下文信息，比如seata,skywalking
        out.writeAttachments(inv.getObjectAttachments());
    }

    /**
     * 编码 响应体
     *
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        Result result = (Result) data;
        // currently, the version value in Response records the version of Request
        /**
         * 根据版本 判断client是否兼容一些attach
         *
         */
        boolean attach = Version.isSupportResponseAttachment(version);
        // 响应结果错误
        Throwable th = result.getException();
        // 无错误
        if (th == null) {
            Object ret = result.getValue();
            // 无结果
            if (ret == null) {
                out.writeByte(attach ? RESPONSE_NULL_VALUE_WITH_ATTACHMENTS : RESPONSE_NULL_VALUE);
            } else {
                // 有结果
                out.writeByte(attach ? RESPONSE_VALUE_WITH_ATTACHMENTS : RESPONSE_VALUE);
                out.writeObject(ret);
            }
        } else {
            // 有错误
            out.writeByte(attach ? RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS : RESPONSE_WITH_EXCEPTION);
            out.writeThrowable(th);
        }

        if (attach) {
            // returns current version of Response to consumer side.
            // 返回服务端的dubbo versoin 版本到客户端
            result.getObjectAttachments().put(DUBBO_VERSION_KEY, Version.getProtocolVersion());
            out.writeAttachments(result.getObjectAttachments());
        }
    }
}
