package org.carlspring.strongbox.controllers.aql;

import org.carlspring.strongbox.config.IntegrationTest;
import org.carlspring.strongbox.providers.layout.Maven2LayoutProvider;
import org.carlspring.strongbox.rest.common.MavenRestAssuredBaseTest;
import org.carlspring.strongbox.storage.repository.MutableRepository;
import org.carlspring.strongbox.storage.repository.RepositoryPolicyEnum;

import java.util.LinkedHashSet;
import java.util.Set;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import static io.restassured.module.mockmvc.RestAssuredMockMvc.given;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * @author sbespalov
 */
@IntegrationTest
@Execution(CONCURRENT)
public class AqlControllerTest extends MavenRestAssuredBaseTest
{


    public static void cleanUp()
        throws Exception
    {
        cleanUp(getRepositoriesToClean());
    }

    @Override
    @BeforeEach
    public void init()
        throws Exception
    {
        super.init();
    }

    public static Set<MutableRepository> getRepositoriesToClean()
    {
        Set<MutableRepository> repositories = new LinkedHashSet<>();
        repositories.add(createRepositoryMock(STORAGE0, "act-releases-1", Maven2LayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "act-releases-2", Maven2LayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "act-releases-3", Maven2LayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "act-releases-4", Maven2LayoutProvider.ALIAS));

        return repositories;
    }

    @Test
    public void testSearchExcludeVersion()
            throws Exception
    {
        String repositoryId = "act-releases-1";

        given().accept(MediaType.APPLICATION_JSON_VALUE)
               .queryParam("query",
                           String.format("storage:%s+repository:%s+groupId:org.carlspring.strongbox.searches+!version:1.0.11.3.1",
                                         STORAGE0,
                                         repositoryId))
               .when()
               .get(getContextBaseUrl() + "/api/aql")
               .then()
               .statusCode(HttpStatus.OK.value())
               // we should have 4 results: 2xjar + 2xpom
               .body("artifact", Matchers.hasSize(4));
    }

    @Test
    public void testBadAqlSyntaxRequest()
            throws Exception
    {
        String repositoryId = "act-releases-2";

        MutableRepository repository = createRepository(STORAGE0,
                                                        repositoryId,
                                                        RepositoryPolicyEnum.RELEASE.getPolicy(),
                                                        true);

        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project2:2.0.11.3:jar");
        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project2:2.0.11.3.1:jar");
        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project2:2.0.11.3.2:jar");

        given().accept(MediaType.APPLICATION_JSON_VALUE)
               .queryParam("query",
                           String.format("storage:%s+repository:%s+groupId:org.carlspring.strongbox.searches-version:2.0.11.3.1",
                                         STORAGE0,
                                         repositoryId))
               .when()
               .get(getContextBaseUrl() + "/api/aql")
               .then()
               .statusCode(HttpStatus.BAD_REQUEST.value())
               .body("error", Matchers.containsString("[1:92]"));
    }

    @Test
    public void testSearchValidMavenCoordinates()
            throws Exception
    {
        String repositoryId = "act-releases-3";

        MutableRepository repository = createRepository(STORAGE0,
                                                        repositoryId,
                                                        RepositoryPolicyEnum.RELEASE.getPolicy(),
                                                        true);

        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project3:3.0.11.3:jar");
        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project3:3.0.11.3.1:jar");
        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project3:3.0.11.3.2:jar");

        given().accept(MediaType.APPLICATION_JSON_VALUE)
               .queryParam("query", "layout:maven+groupId:org.carlspring.strongbox.*+artifactId:test-project3")
               .when()
               .get(getContextBaseUrl() + "/api/aql")
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value())
               .body("artifact", Matchers.hasSize(6));
    }
    
    @Test
    public void testSearchInvalidMavenCoordinates()
            throws Exception
    {
        String repositoryId = "act-releases-4";

        MutableRepository repository = createRepository(STORAGE0,
                                                        repositoryId,
                                                        RepositoryPolicyEnum.RELEASE.getPolicy(),
                                                        true);

        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project4:1.0.11.3:jar");
        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project4:1.0.11.3.1:jar");
        generateArtifact(repository.getBasedir(), "org.carlspring.strongbox.searches:test-project4:1.0.11.3.2:jar");

        given().accept(MediaType.APPLICATION_JSON_VALUE)
               .queryParam("query", "layout:unknown-layout+id:org.carlspring.strongbox.*")
               .when()
               .get(getContextBaseUrl() + "/api/aql")
               .then()
               .log()
               .body()
               .statusCode(HttpStatus.BAD_REQUEST.value())
               .body("error", Matchers.equalTo("Unknown layout [unknown-layout]."));
    }

}
