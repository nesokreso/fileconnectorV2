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

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.Connector;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.util.FTPSiteUtils;
import org.wso2.carbon.connector.util.FileConstants;
import org.wso2.carbon.connector.util.FilePattenMatcher;

public class FileSearch extends AbstractConnector implements Connector {
    private static Log log = LogFactory.getLog(FileSearch.class);

    public void connect(MessageContext messageContext) throws ConnectException {
        String source = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_LOCATION);
        String filePattern = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_PATTERN);
        String dirPattern = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.DIR_PATTERN);
        try {
            readFilesUsingFileSystem(source, filePattern, dirPattern, messageContext);
        } catch (IOException e) {
            handleException(e.getMessage(), messageContext);
        }
    }

    /**
     * Generate the file search
     *
     * @param source         Location fo the file
     * @param filePattern    Pattern of the file
     * @param dirPattern     Pattern of the directory
     * @param messageContext The message context that is processed by a handler in the handle method
     * @throws FileSystemException
     */
    private void readFilesUsingFileSystem(String source, String filePattern, String dirPattern, MessageContext
            messageContext) throws IOException {
        final String FILE_PATTERN;
        final String DIR_PATTERN;
        if (filePattern == null && dirPattern == null) {
            log.error("Both filePattern and dirPattern should not be null, at least one of them should have value.");
        } else {
            try {
                FileSystemOptions opts = FTPSiteUtils.createDefaultOptions();
                FileSystemManager manager = VFS.getManager();
                FileObject remoteFile = manager.resolveFile(source, opts);
                if (remoteFile.exists()) {
                    FileObject[] children = remoteFile.getChildren();
                    FILE_PATTERN = filePattern;
                    DIR_PATTERN = dirPattern;
                    OMFactory factory = OMAbstractFactory.getOMFactory();
                    String outputResult;
                    OMNamespace ns = factory.createOMNamespace(FileConstants.FILECON,
                            FileConstants.NAMESPACE);
                    OMElement result = factory.createOMElement(FileConstants.RESULT, ns);
                    for (FileObject child : children) {
                        if (child.getType() == FileType.FILE && filePattern != null &&
                                new FilePattenMatcher(FILE_PATTERN).validate(child.getName().getBaseName()
                                        .toLowerCase())) {
                            outputResult = child.getName().getBaseName();
                            OMElement messageElement = factory.createOMElement(FileConstants.FILE, ns);
                            messageElement.setText(outputResult);
                            result.addChild(messageElement);
                        } else if (child.getType() == FileType.FOLDER && dirPattern != null &&
                                new FilePattenMatcher(DIR_PATTERN).validate(child.getName().getBaseName()
                                        .toLowerCase())) {
                            outputResult = child.getName().getBaseName();
                            OMElement messageElement = factory.createOMElement(FileConstants.DIR, ns);
                            messageElement.setText(outputResult);
                            result.addChild(messageElement);
                        }
                    }
                    messageContext.getEnvelope().getBody().addChild(result);
                } else {
                    log.error("File location does not exist.");
                }
            } catch (IOException e) {
                handleException("Unable to search a file.", e, messageContext);
            }
        }
    }
}
