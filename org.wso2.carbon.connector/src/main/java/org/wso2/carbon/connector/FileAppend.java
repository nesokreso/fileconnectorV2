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
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.synapse.MessageContext;
import org.codehaus.jettison.json.JSONException;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.Connector;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.util.FTPSiteUtils;
import org.wso2.carbon.connector.util.FileConstants;
import org.wso2.carbon.connector.util.ResultPayloadCreate;

public class FileAppend extends AbstractConnector implements Connector {

    private static final String DEFAULT_ENCODING = "UTF8";
    private static Log log = LogFactory.getLog(FileAppend.class);

    public void connect(MessageContext messageContext) throws ConnectException {
        String source = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_LOCATION);
        String content = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.CONTENT);
        String encoding = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.ENCODING);
        boolean resultStatus = false;
        try {
            resultStatus =
                    appendFile(source, content, encoding,messageContext);
        } catch (IOException e) {
            handleException(e.getMessage(), messageContext);
        }
        generateResult(messageContext, resultStatus);
    }

    /**
     * Generate the result
     *
     * @param messageContext Message context
     * @param resultStatus   true/false
     */
    private void generateResult(MessageContext messageContext, boolean resultStatus) {
        ResultPayloadCreate resultPayload = new ResultPayloadCreate();
        String response = "<result><success>" + resultStatus + "</success></result>";
        OMElement element;
        try {
            element = resultPayload.performSearchMessages(response);
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

    /**
     * @param source   Location if the file
     * @param content  Content that is going to be added
     * @param encoding Encoding type
     * @return true/false
     * @throws IOException
     */
    private boolean appendFile(String source, String content,
                               String encoding,MessageContext messageContext) throws IOException {
        OutputStream out = null;
        boolean resultStatus = false;
        FileObject fileObj = null;
        try {
            FileSystemManager manager = VFS.getManager();
            FileSystemOptions opts = FTPSiteUtils.createDefaultOptions();
            fileObj = manager.resolveFile(source, opts);
            // if the file does not exist, this method creates it
            if (!fileObj.exists()) {
                fileObj.createFile();
            }
            out = fileObj.getContent().getOutputStream(true);
            if (encoding==null) {
                IOUtils.write(content, out, DEFAULT_ENCODING);
            } else {
                IOUtils.write(content, out, encoding);
            }
            resultStatus = true;
        } catch (IOException e) {
            handleException("Unable to append a file.",e, messageContext);
        }finally {
            if (fileObj != null) {
                //close the file object
                fileObj.close();
            }
            if (out != null) {
                //close the output stream
                out.close();
            }
        }
        return resultStatus;
    }
}
