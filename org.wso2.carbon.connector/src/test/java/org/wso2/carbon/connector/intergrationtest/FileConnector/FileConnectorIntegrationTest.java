/**
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * <p/>
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.connector.intergrationtest.FileConnector;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.connector.integration.test.base.ConnectorIntegrationTestBase;
import org.wso2.connector.integration.test.base.RestResponse;

/**
 * Integration test class for file connector
 */
public class FileConnectorIntegrationTest extends ConnectorIntegrationTestBase {

    private Map<String, String> esbRequestHeadersMap = new HashMap<String, String>();

    /**
     * Set up the environment.
     */
    @BeforeClass(alwaysRun = true)
    public void setEnvironment() throws Exception {

        init("fileconnector-connector-2.0.0");
        esbRequestHeadersMap.put("Accept-Charset", "UTF-8");
        esbRequestHeadersMap.put("Content-Type", "application/json");
        esbRequestHeadersMap.put("Accept", "application/json");

        /*esbRequestHeadersMap.put("Content-Type", "application/xml");
        esbRequestHeadersMap.put("Accept", "application/xml");*/
    }

    /**
     * Positive test case for create file method with mandatory parameters.
     */
   /* @Test(groups = { "wso2.esb" }, description = "FileConnector create File/Folder integration test")
    public void testCreateFile() throwsException {
        esbRequestHeadersMap.put("Action", "urn:create");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileCreateMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
       // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for append file method with mandatory parameters.
     */
 /*   @Test(groups = { "wso2.esb" }, description = "FileConnector append file integration test")
    public void testAppendFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:append");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileAppendMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
      //  Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for delete file method with mandatory parameters.
     */
   /* @Test(groups = { "wso2.esb" }, description = "FileConnector delete file integration test")
    public void testDeleteFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:delete");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileDeleteMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
       // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for copy file method with mandatory parameters.
     */
    /*@Test(groups = { "wso2.esb" }, description = "FileConnector copy file integration test")
    public void testCopyFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:copy");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileCopyMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
       // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for read file method with mandatory parameters.
     */
    /*@Test(groups = { "wso2.esb" }, description = "FileConnector read file integration test")
    public void testReadFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:read");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileReadMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
        //Assert.assertEquals(false, esbRestResponse.getBody().toString().isEmpty());
    }*/

    /**
     * Positive test case for archives file method with mandatory parameters.
     */
   /* @Test(groups = { "wso2.esb" }, description = "FileConnector archive file integration test")
    public void testArchiveFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:archive");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileArchiveMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
       // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for unzip method with mandatory parameters.
     */
    /*@Test(groups = { "wso2.esb" }, description = "FileConnector unZip file integration test")
    public void testUnZipFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:unzip");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileUnzipMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
    }*/

    /**
     * Positive test case for fileExist method with mandatory parameters.
     */
   /* @Test(groups = { "wso2.esb" }, description = "FileConnector isFileExist file integration test")
    public void testisFileExistFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:isFileExist");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileExistMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
       // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for listFileZip method with mandatory parameters.
     */
    /*@Test(groups = { "wso2.esb" }, description = "FileConnector listFileZip file integration test")
    public void testListFileZip() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:listFileZip");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileListZipMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
       // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for move method with mandatory parameters.
     */
   /* @Test(groups = { "wso2.esb" }, description = "FileConnector move file integration test")
    public void testMoveFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:move");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileMoveMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
       // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }*/

    /**
     * Positive test case for search method with mandatory parameters.
     */
    @Test(groups = {"wso2.esb"}, description = "FileConnector search file integration test")
    public void testSearchFile() throws Exception {
        esbRequestHeadersMap.put("Action", "urn:search");
        RestResponse<JSONObject> esbRestResponse =
                sendJsonRestRequest(proxyUrl, "POST", esbRequestHeadersMap, "FileSearchMandatory.json");
        Assert.assertEquals(esbRestResponse.getHttpStatusCode(), 200);
        // Assert.assertEquals(true, esbRestResponse.getBody().toString().contains("true"));
    }
}