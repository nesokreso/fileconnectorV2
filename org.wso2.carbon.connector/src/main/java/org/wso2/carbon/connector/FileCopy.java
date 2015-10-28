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
import java.io.InputStream;
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
import org.wso2.carbon.connector.util.FilePattenMatcher;
import org.wso2.carbon.connector.util.ResultPayloadCreate;

public class FileCopy extends AbstractConnector implements Connector {
    private static Log log = LogFactory.getLog(FileCopy.class);

    public void connect(MessageContext messageContext) throws ConnectException {
        String source = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_LOCATION);
        String destination = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.NEW_FILE_LOCATION);
        String filePattern =(String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_PATTERN);
        boolean resultStatus = false;
        try {
            resultStatus = copyFile(source, destination, filePattern,messageContext);
        } catch (IOException e) {
            handleException(e.getMessage(), messageContext);
        }
        ResultPayloadCreate resultPayload = new ResultPayloadCreate();
        generateResults(messageContext, resultStatus, resultPayload);
    }

    /**
     * Generate the results
     *
     * @param messageContext The message context that is processed by a handler in the handle method
     * @param resultStatus   Result of the status (true/false)
     * @param resultPayload  result payload create
     */
    private void generateResults(MessageContext messageContext, boolean resultStatus,
                                 ResultPayloadCreate resultPayload) {
        String response = "<result><copy>" + resultStatus + "</copy></result>";
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
     * Copy files
     *
     * @param fileLocation    Location of the file
     * @param newFileLocation new file location
     * @return return a resultStatus
     */
    private boolean copyFile(String fileLocation, String newFileLocation, String filePattern,MessageContext
            messageContext) throws IOException {
        boolean resultStatus = false;
        FileSystemOptions opts = FTPSiteUtils.createDefaultOptions();
        FileSystemManager manager = VFS.getManager();
        FileObject souFile = manager.resolveFile(fileLocation, opts);
        FileObject destFile = manager.resolveFile(newFileLocation, opts);
        if (filePattern!=null) {
            FileObject[] children = souFile.getChildren();
            for (FileObject child : children) {
                if (child.getType() == FileType.FILE) {
                    copy(fileLocation, newFileLocation, filePattern);
                } else if(child.getType()==FileType.FOLDER){
                    String source = fileLocation + child.getName().getBaseName();
                    copy(source, newFileLocation, filePattern);
                }
            }
            resultStatus = true;
        } else {
            if (souFile.exists()) {
                if (souFile.getType() == FileType.FILE) {
                    InputStream fin = null;
                    OutputStream fOut = null;
                    try {
                        String name = souFile.getName().getBaseName();
                        FileObject outFile = manager.resolveFile(newFileLocation + name, opts);
                        fin = souFile.getContent().getInputStream();
                        fOut = outFile.getContent().getOutputStream();
                            IOUtils.copyLarge(fin, fOut);
                        resultStatus = true;
                    } catch (IOException e) {
                        handleException("Unable to copy.",e,messageContext);
                    }
                     finally {
                        if (fOut != null) {
                            fOut.close();
                        }
                        if (fin != null) {
                            fin.close();
                        }
                    }
                } else {
                    destFile.copyFrom(souFile, Selectors.SELECT_ALL);
                    resultStatus = true;
                }
            } else {
                log.error("The File Location does not exist.");
                resultStatus = false;
            }
        }
        return resultStatus;
    }

    public void copy(String source, String destination, String filePattern)throws IOException{
            FileSystemOptions opts = FTPSiteUtils.createDefaultOptions();
            FileSystemManager manager = VFS.getManager();
            FileObject souFile = manager.resolveFile(source, opts);
            FileObject[] children = souFile.getChildren();
            for (FileObject child : children) {
                try {
                    if (new FilePattenMatcher(filePattern).validate(child.getName().getBaseName())) {
                        String name = child.getName().getBaseName();
                        FileObject outFile = manager.resolveFile(destination + name, opts);
                        outFile.copyFrom(child, Selectors.SELECT_FILES);
                    }
                }
            catch(IOException e) {
                log.error("Error occurred while copying a file. ");
            }
        }
    }
}


