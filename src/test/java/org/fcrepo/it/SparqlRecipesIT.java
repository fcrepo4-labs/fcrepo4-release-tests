package org.fcrepo.it;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
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
 * @author Mike Durbin
 */
public class SparqlRecipesIT {

    public static String FEDORA_CONTEXT = "fcrepo-webapp";
    public static String CONSUMER_CONTEXT = "fcrepo-message-consumer";

    private static String CARGO_PORT = System.getProperty("cargo.port");

    protected static HttpClient client = createClient();

    protected static HttpClient createClient() {
        return HttpClientBuilder.create().setMaxConnPerRoute(MAX_VALUE)
                .setMaxConnTotal(MAX_VALUE).build();
    }

    private static Process fuseki;

    @BeforeClass
    public static void startFuseki() throws InterruptedException, IOException {
        File commandFile = new File("target/jena-fuseki-1.0.1/fuseki-server");
        ProcessBuilder b = new ProcessBuilder().inheritIO()
                .directory(commandFile.getParentFile())
                .command("./fuseki-server", "--update", "--mem", "/test" );
        fuseki = b.start();

        // It might take a while to startup and be ready to receive messages...
        Thread.sleep(10000);

        setUpTestObjects();
    }

    @AfterClass
    public static void stopFuseki() throws InterruptedException {
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
        putDummyDatastream(pid101 + "/master/fcr:content", "application/pdf");

        putObject(pid102);
        markAsIndexable(pid102);
        putDummyDatastream(pid102 + "/master/fcr:content", "text/plain");

        putObject(pid103);
        markAsIndexable(pid103);
        putDummyDatastream(pid103 + "/master/fcr:content", "application/pdf");
        putDummyDatastream(pid103 + "/text/fcr:content", "text/plain");

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
                "    ?ds fcrepo:mixinTypes \"fedora:datastream\" .\n" +
                "    ?ds fcrepo:hasParent ?object . \n" +
                "    filter(str(?ds)=concat(str(?object),'/text')) \n" +
                "}";
        final ByteArrayOutputStream response1b = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery1b).getEntity().writeTo(response1b);
        Assert.assertEquals(Arrays.asList(new String[] { getURIForPid(pid103) }),
                new FusekiResponse(new String(response1b.toByteArray()).trim()).getValues("object"));

        final String fusekiQuery1c = "prefix fcrepo: <http://fedora.info/definitions/v4/repository#>\n" +
                "select ?object where { \n" +
                "    ?ds fcrepo:mixinTypes \"fedora:datastream\" .\n" +
                "    ?ds fcrepo:hasParent ?object . \n" +
                "    ?ds fcrepo:hasContent ?content .\n" +
                "    ?content fcrepo:mimeType \"application/pdf\" \n" +
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
                " { ?obj <http://fedora.info/definitions/v4/rels-ext#isMemberOfCollection> ?col }";
        final ByteArrayOutputStream response3e = new ByteArrayOutputStream();
        queryFuseki(fusekiQuery3e).getEntity().writeTo(response3e);
        final FusekiResponse table = new FusekiResponse(new String(response3e.toByteArray()));
        final HashSet<List<String>> expected = new HashSet<List<String>>();
        expected.add(Arrays.asList(new String[] {"obj", "col"}));
        expected.add(Arrays.asList(new String[] {getURIForPid(pidObj1), getURIForPid(pidCol1)}));
        expected.add(Arrays.asList(new String[] {getURIForPid(pidObj2), getURIForPid(pidCol2)}));
        expected.add(Arrays.asList(new String[] {getURIForPid(pidObj3), getURIForPid(pidCol3)}));
        Assert.assertEquals(expected, new HashSet<List<String>>(table.rows));

        final String fusekiQuery3f = "prefix rels: <http://fedora.info/definitions/v4/rels-ext#>\n" +
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
        final String fusekiQuery3h = "prefix rels: <http://fedora.info/definitions/v4/rels-ext#>\n" +
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
        final String fusekiQuery3h = "prefix rels: <http://fedora.info/definitions/v4/rels-ext#>\n" +
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

    private static String getURIForPid(String pid) {
        return getFedoraBaseUrl() + "/rest/" + pid;
    }

    private static int getStatusCode(final HttpRequestBase request) throws IOException {
        try {
            return client.execute(request).getStatusLine().getStatusCode();
        } finally {
            request.releaseConnection();
        }
    }

    private static void putObject(String pid) throws IOException {
        Assert.assertEquals(201, getStatusCode(new HttpPut(getURIForPid(pid))));
    }

    private static void putDummyDatastream(String path, String mimeType) throws IOException {
        final HttpPut put = new HttpPut(getURIForPid(path));
        put.setHeader("Content-type", mimeType);
        put.setEntity(new StringEntity("garbage"));
        Assert.assertEquals(201, getStatusCode(put));
    }

    private static void updateProperties(String pid, String sparql) throws IOException {
        final HttpPatch patch = new HttpPatch(getURIForPid(pid));
        patch.setHeader("Content-type", "application/sparql-update");
        patch.setEntity(new StringEntity(sparql));
        Assert.assertEquals(204, getStatusCode(patch));
    }

    private static void markAsIndexable(String pid) throws IOException {
        final String sparqlUpdate = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX indexing: <http://fedora.info/definitions/v4/indexing#>\n" +
                "DELETE { }\n" +
                "INSERT {\n" +
                "  <> indexing:hasIndexingTransformation \"default\";\n" +
                "  rdf:type indexing:indexable }\n" +
                "WHERE { }";
        updateProperties(pid, sparqlUpdate);
    }

    private static void setTitle(String pid, String title) throws IOException {
        final String sparqlUpdate = "prefix dc: <http://purl.org/dc/elements/1.1/>" +
                " insert data { <" + getURIForPid(pid) + "> dc:title '" + title + "' . }";
        updateProperties(pid, sparqlUpdate);
    }

    private static void insertIntoCollection(String pid, String collectionPid) throws IOException {
        final String sparqlUpdate = "insert data { <" + getURIForPid(pid) + ">" +
                " <http://fedora.info/definitions/v4/rels-ext#isMemberOfCollection>" +
                " <" + getURIForPid(collectionPid) + "> . }";
        updateProperties(pid, sparqlUpdate);
    }

    private static void linkHierarchicalCollections(String collectionPid, String partPid) throws IOException {
        final String sparqlUpdate = "insert data { <" + getURIForPid(collectionPid) + ">" +
                " <http://fedora.info/definitions/v4/rels-ext#hasPart>" +
                " <" + getURIForPid(partPid) + "> . }";
        updateProperties(collectionPid, sparqlUpdate);
    }

    private static void linkToProject(String pid, String projectPid) throws IOException {
        final String sparqlUpdate = "prefix ex: <http://example.org/>" +
                " insert data { <" + getURIForPid(pid) + ">" +
                " ex:project" +
                " <" + getURIForPid(projectPid) + "> . }";
        updateProperties(pid, sparqlUpdate);

    }

    private static HttpResponse queryFuseki(String query) throws IOException {
        final String queryUrl = "http://localhost:3030/test/query?query=" + URLEncoder.encode(query) + "&default-graph-uri=&output=csv&stylesheet=";
        return client.execute(new HttpGet(queryUrl));
    }

    private static class FusekiResponse {
        final private List<List<String>> rows;

        public FusekiResponse(final String csvResponse) {
            String[] rowArray = csvResponse.split("\\n");
            this.rows = new ArrayList<List<String>>();
            for (String row : rowArray) {
                ArrayList<String> rowList = new ArrayList<String>();
                for (String cell : row.split(",")) {
                    rowList.add(cell.trim());
                }
                this.rows.add(rowList);
            }
        }

        public List<String> getValues(String header) {
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
        return "http://localhost:3030";
    }

    private static String getConsumerBaseUrl() {
        return "http://localhost:" + CARGO_PORT + "/" + CONSUMER_CONTEXT;
    }
}
