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

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.support.RpcUtils;

/**
 * RPC Invocation.
 *
 * @serial Don't change the class name and properties.
 */
public class RpcInvocation implements Invocation, Serializable {

    private static final long serialVersionUID = -4355285085441097045L;

    /**
     * 目标服务唯一名称
     */
    private String targetServiceUniqueName;

    /**
     * 方法名
     */
    private String methodName;

    /**
     * 服务名
     */
    private String serviceName;

    // 参数类型
    private transient Class<?>[] parameterTypes;

    /**
     * 参数类型描述
     */
    private String parameterTypesDesc;

    /**
     * 兼容方法签名
     */
    private String[] compatibleParamSignatures;

    /**
     * 参数
     */
    private Object[] arguments;

    /**
     * 在RPC调用期间，传递到服务端，
     * 算是把一些参数 透传到服务端， 比如skywalking, seata等，用户服务串联
     * Passed to the remote server during RPC call
     */
    private Map<String, Object> attachments;

    /**
     * 仅用于调用方，目前还不知道怎么用
     * Only used on the caller side, will not appear on the wire.
     */
    private Map<Object, Object> attributes = new HashMap<Object, Object>();

    /**
     * 调用实现
     *
     */
    private transient Invoker<?> invoker;

    /**
     * 返回类型
     */
    private transient Class<?> returnType;

    /**
     * 返回类型
     */
    private transient Type[] returnTypes;


    /**
     * 调用方式， 同步  异步 ..
     *
     */
    private transient InvokeMode invokeMode;

    public RpcInvocation() {
    }

    public RpcInvocation(Invocation invocation, Invoker<?> invoker) {
        this(invocation.getMethodName(), invocation.getServiceName(), invocation.getParameterTypes(),
             invocation.getArguments(), new HashMap<>(invocation.getObjectAttachments()),
             invocation.getInvoker(), invocation.getAttributes());
        if (invoker != null) {
            URL url = invoker.getUrl();
            setAttachment(PATH_KEY, url.getPath());
            if (url.hasParameter(INTERFACE_KEY)) {
                setAttachment(INTERFACE_KEY, url.getParameter(INTERFACE_KEY));
            }
            if (url.hasParameter(GROUP_KEY)) {
                setAttachment(GROUP_KEY, url.getParameter(GROUP_KEY));
            }
            if (url.hasParameter(VERSION_KEY)) {
                setAttachment(VERSION_KEY, url.getParameter(VERSION_KEY, "0.0.0"));
            }
            if (url.hasParameter(TIMEOUT_KEY)) {
                setAttachment(TIMEOUT_KEY, url.getParameter(TIMEOUT_KEY));
            }
            if (url.hasParameter(TOKEN_KEY)) {
                setAttachment(TOKEN_KEY, url.getParameter(TOKEN_KEY));
            }
            if (url.hasParameter(APPLICATION_KEY)) {
                setAttachment(APPLICATION_KEY, url.getParameter(APPLICATION_KEY));
            }
        }
        this.targetServiceUniqueName = invocation.getTargetServiceUniqueName();
    }

    public RpcInvocation(Invocation invocation) {
        this(invocation.getMethodName(), invocation.getServiceName(), invocation.getParameterTypes(),
             invocation.getArguments(), invocation.getObjectAttachments(), invocation.getInvoker(), invocation.getAttributes());
        this.targetServiceUniqueName = invocation.getTargetServiceUniqueName();
    }

    public RpcInvocation(Method method, String serviceName, Object[] arguments) {
        this(method, serviceName, arguments, null, null);
    }

    public RpcInvocation(Method method, String serviceName, Object[] arguments, Map<String, Object> attachment, Map<Object, Object> attributes) {
        this(method.getName(), serviceName, method.getParameterTypes(), arguments, attachment, null, attributes);
        this.returnType = method.getReturnType();
    }

    public RpcInvocation(String methodName, String serviceName, Class<?>[] parameterTypes, Object[] arguments) {
        this(methodName, serviceName, parameterTypes, arguments, null, null, null);
    }

    public RpcInvocation(String methodName, String serviceName, Class<?>[] parameterTypes, Object[] arguments, Map<String, Object> attachments) {
        this(methodName, serviceName, parameterTypes, arguments, attachments, null, null);
    }

    /**
     * RpcInvocation 的构造， 可以看到，这里是到方法级别的
     *
     * @param methodName
     * @param serviceName
     * @param parameterTypes
     * @param arguments
     * @param attachments
     * @param invoker
     * @param attributes
     */
    public RpcInvocation(String methodName, String serviceName, Class<?>[] parameterTypes, Object[] arguments,
                         Map<String, Object> attachments, Invoker<?> invoker, Map<Object, Object> attributes) {
        // 方法名
        this.methodName = methodName;
        // 服务名
        this.serviceName = serviceName;
        // 参数类型
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
        // 参数
        this.arguments = arguments == null ? new Object[0] : arguments;
        // 附件
        this.attachments = attachments == null ? new HashMap<>() : attachments;
        // 属性
        this.attributes = attributes == null ? new HashMap<>() : attributes;

        this.invoker = invoker;
        initParameterDesc();
    }

    private void initParameterDesc() {
        ServiceRepository repository = ApplicationModel.getServiceRepository();
        if (StringUtils.isNotEmpty(serviceName)) {
            // 根据服务名，找服务描述
            ServiceDescriptor serviceDescriptor = repository.lookupService(serviceName);
            if (serviceDescriptor != null) {
                // 根据方法名，找方法描述
                MethodDescriptor methodDescriptor = serviceDescriptor.getMethod(methodName, parameterTypes);
                if (methodDescriptor != null) {
                    // 方法参数描述
                    this.parameterTypesDesc = methodDescriptor.getParamDesc();
                    // 兼容
                    this.compatibleParamSignatures = methodDescriptor.getCompatibleParamSignatures();
                    // 返回类型
                    this.returnTypes = methodDescriptor.getReturnTypes();
                }
            }
        }

        //parameterTypesDesc 如果为空，则本地生成
        if (parameterTypesDesc == null) {
            this.parameterTypesDesc = ReflectUtils.getDesc(this.getParameterTypes());
            this.compatibleParamSignatures = Stream.of(this.parameterTypes).map(Class::getName).toArray(String[]::new);
            this.returnTypes = RpcUtils.getReturnTypes(this);
        }
    }

    @Override
    public Invoker<?> getInvoker() {
        return invoker;
    }

    public void setInvoker(Invoker<?> invoker) {
        this.invoker = invoker;
    }

    public Object put(Object key, Object value) {
        return attributes.put(key, value);
    }

    public Object get(Object key) {
        return attributes.get(key);
    }

    @Override
    public Map<Object, Object> getAttributes() {
        return attributes;
    }

    @Override
    public String getTargetServiceUniqueName() {
        return targetServiceUniqueName;
    }

    public void setTargetServiceUniqueName(String targetServiceUniqueName) {
        this.targetServiceUniqueName = targetServiceUniqueName;
    }

    @Override
    public String getMethodName() {
        return methodName;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
    }

    public String getParameterTypesDesc() {
        return parameterTypesDesc;
    }

    public void setParameterTypesDesc(String parameterTypesDesc) {
        this.parameterTypesDesc = parameterTypesDesc;
    }

    public String[] getCompatibleParamSignatures() {
        return compatibleParamSignatures;
    }

    // parameter signatures can be set independently, it is useful when the service type is not found on caller side and
    // the invocation is not generic invocation either.
    public void setCompatibleParamSignatures(String[] compatibleParamSignatures) {
        this.compatibleParamSignatures = compatibleParamSignatures;
    }

    @Override
    public Object[] getArguments() {
        return arguments;
    }

    public void setArguments(Object[] arguments) {
        this.arguments = arguments == null ? new Object[0] : arguments;
    }

    @Override
    public Map<String, Object> getObjectAttachments() {
        return attachments;
    }

    @Deprecated
    @Override
    public Map<String, String> getAttachments() {
        return new AttachmentsAdapter.ObjectToStringMap(attachments);
    }

    @Deprecated
    public void setAttachments(Map<String, String> attachments) {
        this.attachments = attachments == null ? new HashMap<>() : new HashMap<>(attachments);
    }

    public void setObjectAttachments(Map<String, Object> attachments) {
        this.attachments = attachments == null ? new HashMap<>() : attachments;
    }

    public void setAttachment(String key, Object value) {
        if (attachments == null) {
            attachments = new HashMap<>();
        }
        attachments.put(key, value);
    }

    public void setAttachmentIfAbsent(String key, Object value) {
        if (attachments == null) {
            attachments = new HashMap<>();
        }
        if (!attachments.containsKey(key)) {
            attachments.put(key, value);
        }
    }

    @Deprecated
    public void addAttachments(Map<String, String> attachments) {
        if (attachments == null) {
            return;
        }
        if (this.attachments == null) {
            this.attachments = new HashMap<>();
        }
        this.attachments.putAll(attachments);
    }

    public void addObjectAttachments(Map<String, Object> attachments) {
        if (attachments == null) {
            return;
        }
        if (this.attachments == null) {
            this.attachments = new HashMap<>();
        }
        this.attachments.putAll(attachments);
    }

    @Deprecated
    public void addAttachmentsIfAbsent(Map<String, String> attachments) {
        if (attachments == null) {
            return;
        }
        for (Map.Entry<String, String> entry : attachments.entrySet()) {
            setAttachmentIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    public void addObjectAttachmentsIfAbsent(Map<String, Object> attachments) {
        if (attachments == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : attachments.entrySet()) {
            setAttachmentIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    @Override
    @Deprecated
    public String getAttachment(String key) {
        if (attachments == null) {
            return null;
        }
        Object value = attachments.get(key);
        if (value instanceof String) {
            return (String) value;
        }
        return null;
    }

    @Override
    public Object getObjectAttachment(String key) {
        if (attachments == null) {
            return null;
        }
        return attachments.get(key);
    }

    @Override
    @Deprecated
    public String getAttachment(String key, String defaultValue) {
        if (attachments == null) {
            return defaultValue;
        }
        Object value = attachments.get(key);
        if (value instanceof String) {
            String strValue = (String) value;
            if (StringUtils.isEmpty(strValue)) {
                return defaultValue;
            } else {
                return strValue;
            }
        }
        return null;
    }

    @Deprecated
    public Object getObjectAttachment(String key, Object defaultValue) {
        if (attachments == null) {
            return defaultValue;
        }
        Object value = attachments.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    public Type[] getReturnTypes() {
        return returnTypes;
    }

    public void setReturnTypes(Type[] returnTypes) {
        this.returnTypes = returnTypes;
    }

    public InvokeMode getInvokeMode() {
        return invokeMode;
    }

    public void setInvokeMode(InvokeMode invokeMode) {
        this.invokeMode = invokeMode;
    }

    @Override
    public String toString() {
        return "RpcInvocation [methodName=" + methodName + ", parameterTypes="
                + Arrays.toString(parameterTypes) + ", arguments=" + Arrays.toString(arguments)
                + ", attachments=" + attachments + "]";
    }

}
