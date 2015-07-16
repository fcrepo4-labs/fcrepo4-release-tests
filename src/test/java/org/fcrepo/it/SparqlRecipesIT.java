/**
 * Copyright 2015 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.it;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.lang.System.getProperty;
import static java.lang.Integer.parseInt;
import static java.lang.Integer.MAX_VALUE;

/**
 * An integration test to test the interactions between  fuseki,
 * the fedora-message-consumer and fcrepo4-webapp.  The later two are
 * brought up by maven using the cargo plugin while fuseki is launched
 * as a process within this test.
 *
 * The tests mirror the manual tests described on the Fedora 4 wiki.
 * https://wiki.duraspace.org/display/FF/SPARQL+Recipes
 *
 * @author lsitu
 * @author Mike Durbin
 * @since Sep 19, 2014
 */
public class SparqlRecipesIT {

    public static String FEDORA_CONTEXT = "fcrepo-webapp";
    public static String CONSUMER_CONTEXT = "fcrepo-message-consumer";

    private static String CARGO_PORT = System.getProperty("fcrepo.dynamic.test.port", "8080");
    private static final int FUSEKI_PORT = parseInt(getProperty("fuseki.dynamic.test.port", "3030"));
    private static final int MGT_PORT = parseInt(getProperty("fuseki.dynamic.mgt.port", "3031"));
    private static final String serverAddress = "http://localhost:" + MGT_PORT + "/mgt";

    private static short FCREPO_SNAPSHOT_NUMBER = 4;
    private static String DATASTREAM_URL_SUFIX = "/fcr:metadata";
    private static String DATASTREAM_CONTENT_URL_SUFIX = "";
    private static String DATASTREAM_MIXIN_TYPE = "fedora:NonRdfSourceDescription";
    private static String DATASTREAM_RELATION = "<http://www.iana.org/assignments/relation/describes>";

    private static HttpClient client = createClient();

    private static HttpClient createClient() {
        return HttpClientBuilder.create().setMaxConnPerRoute(MAX_VALUE)
                .setMaxConnTotal(MAX_VALUE).build();
    }

    private static Process fuseki;

    @BeforeClass
    public static void startFuseki() throws InterruptedException, IOException {

        //Determine the snapshot used for testing and its REST api,
        //make it default for snapshot 4 for the current version
        final String fcrepoSnapshot = System.getProperty("fcrepo.version");
        if (fcrepoSnapshot != null
                && fcrepoSnapshot.indexOf("-") > 0
                && fcrepoSnapshot.indexOf("4.0.0-beta") >= 0) {
            final String[] verTokens = fcrepoSnapshot.split("-");
            if (verTokens.length >= 3) {
                try {
                     FCREPO_SNAPSHOT_NUMBER = Short.parseShort(verTokens[2]);
                } catch (final NumberFormatException ne) {
                    FCREPO_SNAPSHOT_NUMBER = 4;
                }
            }
        }
        if (FCREPO_SNAPSHOT_NUMBER < 4) {
            DATASTREAM_URL_SUFIX = "";
            DATASTREAM_CONTENT_URL_SUFIX = "/fcr:content";
            DATASTREAM_MIXIN_TYPE = "fedora:datastream";
            DATASTREAM_RELATION = "fcrepo:hasContent";
        }

        final File commandFile = new File("target/jena-fuseki-1.0.1/fuseki-server");
        final ProcessBuilder b = new ProcessBuilder().inheritIO()
                .directory(commandFile.getParentFile())
                .command("./fuseki-server", "--update", "--mem", "--port=" + FUSEKI_PORT,
                        "--mgtPort=" + MGT_PORT, "/test" );
        fuseki = b.start();

        // It might take a while to startup and be ready to receive messages...
        Thread.sleep(10000);

        setUpTestObjects();
    }

    @AfterClass
    public static void stopFuseki() throws InterruptedException {
        final HttpPost method = new HttpPost(serverAddress + "?cmd=shutdown");
        try {
            client.execute(method);
        } catch (final IOException e) {
            // do nothing
        }
        fuseki.destroy();
    }

    @Test
    public void testFusekiIsRunning() throws IOException {
        Assert.assertEquals("Fuseki must be running!",
                200, getStatusCode(new HttpGet(getFusekiBaseUrl())));
    }

    @Test
    public void testFedoraIsRunning() throws IOException {
        Assert.assertEquals("Fedora must be running!",
                200, getStatusCode(new HttpGet(getFedoraBaseUrl())));
    }

    private boolean initialized = false;

    private static final String pid101 = "objects/101";
    private static final String pid102 = "objects/102";
    private static final String pid103 = "objects/103";

    private static final String title = "foo";
    private static final String pid201 = "objects/201";

    private static final String pidCol1 = "objects/col1";
    private static final String pidCol2 = "objects/col2";
    private static final String pidCol3 = "objects/col3";
    private static final String pidObj1 = "objects/obj1";
    private static final String pidObj2 = "objects/obj2";
    private static final String pidObj3 = "objects/obj3";

    private static final String pidProj1 = "objects/proj1";

    public static void setUpTestObjects() throws IOException, InterruptedException {
        System.out.println("Adding test objects...");
        putObject(pid101);
        markAsIndexable(pid101);
        putDummyDatastream(pid101 + "/master" + DATASTREAM_CONTENT_URL_SUFIX, "application/pdf");
        markAsIndexable(pid101 + "/master" + DATASTREAM_URL_SUFIX);

        putObject(pid102);
        markAsIndexable(pid102);
        putDummyDatastream(pid102 + "/master" + DATASTREAM_CONTENT_URL_SUFIX, "text/plain");
        markAsIndexable(pid102 + "/master" + DATASTREAM_URL_SUFIX);

        putObject(pid103);
        markAsIndexable(pid103);
        putDummyDatastream(pid103 + "/master" + DATASTREAM_CONTENT_URL_SUFIX, "application/pdf");
        putDummyDatastream(pid103 + "/text" + DATASTREAM_CONTENT_URL_SUFIX, "text/plain");
        markAsIndexable(pid103 + "/text" + DATASTREAM_URL_SUFIX);
        markAsIndexable(pid103 + "/master" + DATASTREAM_URL_SUFIX);

        putObject(pid201);
        markAsIndexable(pid201);
        setTitle(pid201, title);

        putObject(pidCol1);
        markAsIndexable(pidCol1);

        putObject(pidCol2);
        markAsIndexable(pidCol2);

        putObject(pidCol3);
        markAsIndexable(pidCol3);

        putObject(pidObj1);
        markAsIndexable(pidObj1);
        insertIntoCollection(pidObj1, pidCol1);

        putObject(pidObj2);
        markAsIndexable(pidObj2);
        insertIntoCollection(pidObj2, pidCol2);

        putObject(pidObj3);
        markAsIndexable(pidObj3);
        insertIntoCollection(pidObj3, pidCol3);

        linkHierarchicalCollections(pidCol1, pidCol2);
        linkHierarchicalCollections(pidCol2, pidCol3);

        putObject(pidProj1);
        markAsIndexable(pidProj1);
        linkToProject(pidObj1, pidProj1);

        // it might take a while for all the updates to be processed...
        Thread.sleep(15000);
    }

    @Test
    public void testSparqlQueries1() throws IOException, InterruptedException {
        final String fusekiQuery1b = "prefix fcrepo: <http://fedora.info/definitions/v4/repository#>\n" +
                "select ?object where { \n" +
                "    ?ds fcrepo:mixinTypes \"" + DATASTREAM_MIXIN_TYPE + "\" .\n" +
                "    ?ds fcrepo:hasParent ?object . \n" +
                "    filter(str(?ds)=concat(str(?object),'/text" + DATASTREAM_URL_SUFIX + "')) \n" +
                "}";
        final ByteArrayOutputStream response1b = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery1b).getEntity().writeTo(response1b);
        Assert.assertEquals(Arrays.asList(new String[] { getURIForPid(pid103) }),
                new FusekiResponse(new String(response1b.toByteArray()).trim()).getValues("object"));

        final String fusekiQuery1c = "prefix fcrepo: <http://fedora.info/definitions/v4/repository#>\n" +
                "prefix ebucore: <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#>\n" +
                "select ?object where { \n" +
                "    ?ds fcrepo:mixinTypes \"" + DATASTREAM_MIXIN_TYPE + "\" .\n" +
                "    ?ds fcrepo:hasParent ?object . \n" +
                "    ?ds " + DATASTREAM_RELATION + " ?content .\n" +
                "    ?content ebucore:hasMimeType \"application/pdf\" \n" +
                "}";
        final ByteArrayOutputStream response1c = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery1c).getEntity().writeTo(response1c);
        Assert.assertEquals(new HashSet(Arrays.asList(new String[] { getURIForPid(pid101), getURIForPid(pid103) })),
                new HashSet(new FusekiResponse(new String(response1c.toByteArray()).trim()).getValues("object")));
    }

    @Test
    public void testSparqlQueries2() throws IOException {
        final String fusekiQuery2b = "prefix dc: <http://purl.org/dc/elements/1.1/>\n" +
                "select ?object ?title where { ?object dc:title ?title }";
        final ByteArrayOutputStream response2b = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery2b).getEntity().writeTo(response2b);
        Assert.assertEquals(new HashSet(Arrays.asList(new String[] { getURIForPid(pid201) })),
                new HashSet(new FusekiResponse(new String(response2b.toByteArray()).trim()).getValues("object")));
        Assert.assertEquals(new HashSet(Arrays.asList(new String[] { title })),
                new HashSet(new FusekiResponse(new String(response2b.toByteArray()).trim()).getValues("title")));

        final String fusekiQuery3e = "select ?obj ?col where" +
                " { ?obj <http://some-vocabulary.org/rels-ext#isMemberOfCollection> ?col }";
        final ByteArrayOutputStream response3e = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery3e).getEntity().writeTo(response3e);
        final FusekiResponse table = new FusekiResponse(new String(response3e.toByteArray()));
        final HashSet<List<String>> expected = new HashSet<List<String>>();
        expected.add(Arrays.asList(new String[] {"obj", "col"}));
        expected.add(Arrays.asList(new String[] {getURIForPid(pidObj1), getURIForPid(pidCol1)}));
        expected.add(Arrays.asList(new String[] {getURIForPid(pidObj2), getURIForPid(pidCol2)}));
        expected.add(Arrays.asList(new String[] {getURIForPid(pidObj3), getURIForPid(pidCol3)}));
        Assert.assertEquals(expected, new HashSet<List<String>>(table.rows));

        final String fusekiQuery3f = "prefix rels: <http://some-vocabulary.org/rels-ext#>\n" +
                "select ?obj where {\n" +
                "  <" + getURIForPid(pidCol1) + "> rels:hasPart* ?col\n" +
                "  . ?obj rels:isMemberOfCollection ?col\n" +
                "}";
        final ByteArrayOutputStream response3f = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery3f).getEntity().writeTo(response3f);
        Assert.assertEquals(new HashSet(Arrays.asList(new String[] {
                getURIForPid(pidObj1), getURIForPid(pidObj2), getURIForPid(pidObj3) })),
                new HashSet(new FusekiResponse(new String(response3f.toByteArray()).trim()).getValues("obj")));
    }

    @Test
    public void testSparqlQueries3() throws IOException {
        final String fusekiQuery3h = "prefix rels: <http://some-vocabulary.org/rels-ext#>\n" +
                "prefix ex: <http://example.org/>\n" +
                "select ?obj where {\n" +
                "  { ?obj ex:project <" + getURIForPid(pidProj1) + "> }\n" +
                "  UNION\n" +
                "  { ?obj rels:isMemberOfCollection <" + getURIForPid(pidCol2) + "> }\n" +
                "}";
        final ByteArrayOutputStream response3h = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery3h).getEntity().writeTo(response3h);
        Assert.assertEquals(new HashSet(Arrays.asList(new String[] {
                getURIForPid(pidObj1), getURIForPid(pidObj2) })),
                new HashSet(new FusekiResponse(new String(response3h.toByteArray()).trim()).getValues("obj")));
    }

    @Test
    public void testCountObjectsLinkedToCollectionOrProject() throws IOException {
        final String fusekiQuery3h = "prefix rels: <http://some-vocabulary.org/rels-ext#>\n" +
                "prefix ex: <http://example.org/>\n" +
                "select (count(distinct ?obj) as ?count) where {\n" +
                "  { ?obj ex:project <" + getURIForPid(pidProj1) + "> }\n" +
                "  UNION\n" +
                "  { ?obj rels:isMemberOfCollection <" + getURIForPid(pidCol2) + "> }\n" +
                "}";
        final ByteArrayOutputStream response3h = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery3h).getEntity().writeTo(response3h);
        Assert.assertEquals(Arrays.asList(new String[] { "2" }),
               new FusekiResponse(new String(response3h.toByteArray()).trim()).getValues("count"));
    }

    private static String getURIForPid(final String pid) {
        return getFedoraBaseUrl() + "/rest/" + pid;
    }

    private static int getStatusCode(final HttpRequestBase request) throws IOException {
        try {
            return client.execute(request).getStatusLine().getStatusCode();
        } finally {
            request.releaseConnection();
        }
    }

    private static void putObject(final String pid) throws IOException {
        Assert.assertEquals(201, getStatusCode(new HttpPut(getURIForPid(pid))));
    }

    private static void putDummyDatastream(final String path, final String mimeType) throws IOException {
        final HttpPut put = new HttpPut(getURIForPid(path));
        put.setHeader("Content-type", mimeType);
        put.setEntity(new StringEntity("garbage"));
        Assert.assertEquals(201, getStatusCode(put));
    }

    private static void updateProperties(final String pid, final String sparql) throws IOException {
        final HttpPatch patch = new HttpPatch(getURIForPid(pid));
        patch.setHeader("Content-type", "application/sparql-update");
        patch.setEntity(new StringEntity(sparql));
        Assert.assertEquals(204, getStatusCode(patch));
    }

    private static void markAsIndexable(final String pid) throws IOException {
        final String sparqlUpdate = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX indexing: <http://fedora.info/definitions/v4/indexing#>\n" +
                "DELETE { }\n" +
                "INSERT {\n" +
                "  <> indexing:hasIndexingTransformation \"default\";\n" +
                "  rdf:type indexing:Indexable }\n" +
                "WHERE { }";
        updateProperties(pid, sparqlUpdate);
    }

    private static void setTitle(final String pid, final String title) throws IOException {
        final String sparqlUpdate = "prefix dc: <http://purl.org/dc/elements/1.1/>" +
                " insert data { <" + getURIForPid(pid) + "> dc:title '" + title + "' . }";
        updateProperties(pid, sparqlUpdate);
    }

    private static void insertIntoCollection(final String pid, final String collectionPid) throws IOException {
        final String sparqlUpdate = "insert data { <" + getURIForPid(pid) + ">" +
                " <http://some-vocabulary.org/rels-ext#isMemberOfCollection>" +
                " <" + getURIForPid(collectionPid) + "> . }";
        updateProperties(pid, sparqlUpdate);
    }

    private static void linkHierarchicalCollections(final String collectionPid,
                                                    final String partPid) throws IOException {
        final String sparqlUpdate = "insert data { <" + getURIForPid(collectionPid) + ">" +
                " <http://some-vocabulary.org/rels-ext#hasPart>" +
                " <" + getURIForPid(partPid) + "> . }";
        updateProperties(collectionPid, sparqlUpdate);
    }

    private static void linkToProject(final String pid, final String projectPid) throws IOException {
        final String sparqlUpdate = "prefix ex: <http://example.org/>" +
                " insert data { <" + getURIForPid(pid) + ">" +
                " ex:project" +
                " <" + getURIForPid(projectPid) + "> . }";
        updateProperties(pid, sparqlUpdate);

    }

    private static HttpResponse queryFuseki(final String query) throws IOException {
        final String queryUrl = getFusekiBaseUrl() + "/test/query?query=" + URLEncoder.encode(query) +
                "&default-graph-uri=&output=csv&stylesheet=";
        return client.execute(new HttpGet(queryUrl));
    }

    private static class FusekiResponse {
        final private List<List<String>> rows;

        public FusekiResponse(final String csvResponse) {
            final String[] rowArray = csvResponse.split("\\n");
            this.rows = new ArrayList<List<String>>();
            for (final String row : rowArray) {
                final ArrayList<String> rowList = new ArrayList<String>();
                for (final String cell : row.split(",")) {
                    rowList.add(cell.trim());
                }
                this.rows.add(rowList);
            }
        }

        public List<String> getValues(final String header) {
            final List<String> results = new ArrayList<String>();
            int columnIndex = -1;
            if (!rows.isEmpty()) {
                final List<String> headerRow = rows.get(0);
                for (int i = 0; i < headerRow.size(); i ++) {
                    if (headerRow.get(i).equals(header)) {
                        columnIndex = i;
                    }
                }
                if (columnIndex != -1) {
                    for (int rowIndex = 1; rowIndex < rows.size(); rowIndex ++) {
                        results.add(rows.get(rowIndex).get(columnIndex));
                    }
                }
            }
            return results;
        }

        public List<List<String>> getRows() {
            return this.rows;
        }
    }

    private static String getFedoraBaseUrl() {
        return "http://localhost:" + CARGO_PORT + "/" + FEDORA_CONTEXT;
    }

    private static String getFusekiBaseUrl() {
        return "http://localhost:" + FUSEKI_PORT;
    }

    private static String getConsumerBaseUrl() {
        return "http://localhost:" + CARGO_PORT + "/" + CONSUMER_CONTEXT;
    }
}
