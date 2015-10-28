/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * 
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.connector;

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMElement;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.synapse.MessageContext;
import org.codehaus.jettison.json.JSONException;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.Connector;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.util.FTPSiteUtils;
import org.wso2.carbon.connector.util.FileConstants;
import org.wso2.carbon.connector.util.ResultPayloadCreate;

public class FileCreate extends AbstractConnector implements Connector {
    private static final String DEFAULT_ENCODING = "UTF8";
    private static Log log = LogFactory.getLog(FileCreate.class);

    /**
     * @param messageContext The message context that is processed by a handler in the handle method
     */
    public void connect(MessageContext messageContext) throws ConnectException {
        String source = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_LOCATION);
        String content =(String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.CONTENT);
        String encoding =(String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.ENCODING);
        if (log.isDebugEnabled()) {
            log.info("File creation started...");
        }
        boolean resultStatus = false;
        try {
            resultStatus = createFile(source, content, encoding);
        } catch (IOException e) {
            log.error(e.getMessage());
            handleException(e.getMessage(), messageContext);
        }
        generateOutput(messageContext, resultStatus);
    }

    /**
     * Create a file with Apache commons
     *
     * @param source   Location of the file/folder
     * @param content  Content in a file
     * @param encoding Encoding type
     * @return Return the status
     */
    private boolean createFile(String source, String content,
                               String encoding) throws IOException {
        boolean resultStatus = false;
        FileSystemOptions opts = FTPSiteUtils.createDefaultOptions();
        OutputStream out = null;
        FileSystemManager manager = VFS.getManager();
        if (manager != null) {
            FileObject sourceFile = manager.resolveFile(source, opts);
            try {
                if (isFolder(sourceFile)) {
                    sourceFile.createFolder();
                } else {
                    if (content==null) {
                        sourceFile.createFile();
                    } else {
                        FileContent fileContent = sourceFile.getContent();
                        out = fileContent.getOutputStream(true);
                        if (encoding==null) {
                            IOUtils.write(content, out, DEFAULT_ENCODING);
                        } else {
                            IOUtils.write(content, out, encoding);
                        }
                    }
                }
                resultStatus = true;
            } catch (IOException e) {
                log.error("Unable to create a file/folder.", e);
            } finally {
                if (sourceFile != null) {
                    sourceFile.close();
                }
                if (out != null) {
                    out.close();
                }
            }
        }
        return resultStatus;
    }

    public boolean isFolder(FileObject remoteFile) {
        boolean isFolder = false;
        if (remoteFile.getName().getExtension().equals("")) {
            isFolder = true;
        }
        return isFolder;
    }

    /**
     * Generate the output payload
     *
     * @param messageContext The message context that is processed by a handler in the handle method
     * @param resultStatus   Result of the status (true/false)
     */
    private void generateOutput(MessageContext messageContext, boolean resultStatus) {
        ResultPayloadCreate resultPayload = new ResultPayloadCreate();
        String response = "<result><success>" + resultStatus + "</success></result>";

        try {
            OMElement element = resultPayload.performSearchMessages(response);
            resultPayload.preparePayload(messageContext, element);
        } catch (XMLStreamException e) {
            log.error(e.getMessage());
            handleException(e.getMessage(), messageContext);
        } catch (IOException e) {
            log.error(e.getMessage());
            handleException(e.getMessage(), messageContext);
        } catch (JSONException e) {
            log.error(e.getMessage());
            handleException(e.getMessage(), messageContext);
        }
    }
}
