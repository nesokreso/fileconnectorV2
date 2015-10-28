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

import java.io.*;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.xml.stream.XMLStreamException;
import org.apache.axiom.om.OMElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.codehaus.jettison.json.JSONException;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.Connector;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.util.*;

public class FileArchives extends AbstractConnector implements Connector {
    private static Log log = LogFactory.getLog(FileArchives.class);
    byte[] bytes = new byte[FileConstants.BUFFER_SIZE];

    public void connect(MessageContext messageContext) throws ConnectException {
        String source = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.FILE_LOCATION);
        String destinstion = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                FileConstants.NEW_FILE_LOCATION);
        boolean resultStatus = false;
        try {
            resultStatus = fileCompress(messageContext, source, destinstion);
        } catch (Exception e) {
            handleException(e.getMessage(), messageContext);
        }
        generateResults(messageContext, resultStatus);
    }

    /**
     * @param messageContext The message context that is processed by a handler in the handle method
     * @param source         The file to be archived
     * @param destination    Destination of the archived file
     * @return return status
     * @throws SynapseException
     */
    public boolean fileCompress(MessageContext messageContext, String source, String destination) throws
            SynapseException {
        boolean resultStatus;
        try {
            FileSystemManager fsManager = VFS.getManager();
            FileSystemOptions opts = FTPSiteUtils.createDefaultOptions();
            FileObject fileObj = fsManager.resolveFile(source, opts);
            FileObject destObj = fsManager.resolveFile(destination, opts);
            if (fileObj.exists()) {
                if (fileObj.getType() == FileType.FOLDER) {
                    List<FileObject> fileList = new ArrayList<FileObject>();
                    getAllFiles(fileObj,fileList);
                    writeZipFiles(fileObj, destObj, fileList);
                } else {
                    ZipOutputStream zos = null;
                    InputStream fin = null;
                    try {
                        zos = new ZipOutputStream(destObj.getContent().getOutputStream());
                        fin = fileObj.getContent().getInputStream();
                        ZipEntry zipEntry = new ZipEntry(fileObj.getName().getBaseName());
                        zos.putNextEntry(zipEntry);
                        int length;
                        while ((length = fin.read(bytes)) != -1) {
                            zos.write(bytes, 0, length);
                        }
                    } catch (Exception e) {
                        log.error("Unable to compress a file.", e);
                    } finally {
                        if (zos != null) {
                            zos.close();
                        }
                        if (fin != null) {
                            fin.close();
                        }
                    }
                }
                resultStatus = true;
            } else {
                log.error("The File location does not exist.");
                resultStatus = false;
            }
        } catch (IOException e) {
            resultStatus = false;
            log.error("Unable to process the zip file", e);
            handleException(e.getMessage(), messageContext);
        }
        return resultStatus;
    }

    /**
     * @param dir      source file directory
     * @param fileList list of file inside directory
     * @throws IOException
     */
    public void getAllFiles(FileObject dir, List<FileObject> fileList) throws IOException {
        try {
            FileObject[] children = dir.getChildren();
            for (FileObject child : children) {
                fileList.add(child);
                if (child.getType() == FileType.FOLDER) {
                    getAllFiles(child, fileList);
                }
            }
        } catch (Exception e) {
            log.error("Unable to get all files.", e);
        }
    }

    /**
     * @param fileObj        source fileObject
     * @param directoryToZip destination fileObject
     * @param fileList       list of files to be compressed
     * @throws IOException
     */
    public void writeZipFiles(FileObject fileObj, FileObject directoryToZip, List<FileObject> fileList) throws
            IOException {
        ZipOutputStream zos = null;
        try {
            zos = new ZipOutputStream(directoryToZip.getContent().getOutputStream());
            for (FileObject file : fileList) {
                if (file.getType() == FileType.FILE) {
                    addToZip(fileObj, file, zos);
                }
            }
        } catch (IOException e) {
            log.error("Error occur in writing files", e);
        } finally {
            if (zos != null) {
                zos.close();
            }
        }
    }

    /**
     * @param fileObject Source fileObject
     * @param file       The file inside source folder
     * @param zos        ZipOutputStream
     */
    public void addToZip(FileObject fileObject, FileObject file, ZipOutputStream zos){
        InputStream fin = null;
        try {
            fin = file.getContent().getInputStream();
            String entry = file.getName().toString().substring(fileObject.getName().toString().length() + 1,
                    file.getName().toString().length());
            ZipEntry zipEntry = new ZipEntry(entry);
            zos.putNextEntry(zipEntry);
            int length;
            while ((length = fin.read(bytes)) != -1) {
                zos.write(bytes, 0, length);
            }
        } catch (IOException e) {
            log.error("Unable to add a file in to zip file directory.", e);
        } finally {
            try {
                zos.closeEntry();
                if (fin != null) {
                    fin.close();
                }
            }catch (IOException e){
                log.warn("Error occurred :"+e.getMessage(),e);
            }
        }
    }

    /**
     * Generate the results
     *
     * @param messageContext message context
     * @param resultStatus   output result (true/false)
     */
    private void generateResults(MessageContext messageContext, boolean resultStatus) {
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
