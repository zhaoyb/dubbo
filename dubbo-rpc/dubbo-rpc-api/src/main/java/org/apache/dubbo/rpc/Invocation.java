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
package org.apache.dubbo.rpc;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Invocation. (API, Prototype, NonThreadSafe)
 *
 * @serial Don't change the class name and package name.
 * @see org.apache.dubbo.rpc.Invoker#invoke(Invocation)
 * @see org.apache.dubbo.rpc.RpcInvocation
 */
public interface Invocation {

    String getTargetServiceUniqueName();

    /**
     * 返回方法名
     *
     * get method name.
     *
     * @return method name.
     * @serial
     */
    String getMethodName();


    /**
     * 返回接口名
     *
     * get the interface name
     * @return
     */
    String getServiceName();

    /**
     *
     * 返回参数类型数组
     * get parameter types.
     *
     * @return parameter types.
     * @serial
     */
    Class<?>[] getParameterTypes();

    /**
     * 方法签名
     *
     * get parameter's signature, string representation of parameter types.
     *
     * @return parameter's signature
     */
    default String[] getCompatibleParamSignatures() {
        return Stream.of(getParameterTypes())
                .map(Class::getName)
                .toArray(String[]::new);
    }

    /**
     * 返回参数
     *
     * get arguments.
     *
     * @return arguments.
     * @serial
     */
    Object[] getArguments();

    /**
     * 消息附件
     *
     * get attachments.
     *
     * @return attachments.
     * @serial
     */
    @Deprecated
    Map<String, String> getAttachments();

    Map<String, Object> getObjectAttachments();

    void setAttachment(String key, Object value);

    void setAttachmentIfAbsent(String key, Object value);

    /**
     * get attachment by key.
     *
     * @return attachment value.
     * @serial
     */
    String getAttachment(String key);

    Object getObjectAttachment(String key);

    /**
     * get attachment by key with default value.
     *
     * @return attachment value.
     * @serial
     */
    String getAttachment(String key, String defaultValue);

    Object getObjectAttachment(String key, Object defaultValue);

    /**
     * 在当前上下文中获取调用者
     *
     * get the invoker in current context.
     *
     * @return invoker.
     * @transient
     */
    Invoker<?> getInvoker();

    Object put(Object key, Object value);

    Object get(Object key);

    Map<Object, Object> getAttributes();
}