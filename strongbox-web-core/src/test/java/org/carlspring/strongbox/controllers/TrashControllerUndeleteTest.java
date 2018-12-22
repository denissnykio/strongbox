package org.carlspring.strongbox.controllers;

import org.carlspring.strongbox.config.IntegrationTest;
import org.carlspring.strongbox.providers.layout.Maven2LayoutProvider;
import org.carlspring.strongbox.rest.common.MavenRestAssuredBaseTest;
import org.carlspring.strongbox.storage.repository.MavenRepositoryFactory;
import org.carlspring.strongbox.storage.repository.MutableRepository;
import org.carlspring.strongbox.xml.configuration.repository.MutableMavenRepositoryConfiguration;

import javax.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import static io.restassured.module.mockmvc.RestAssuredMockMvc.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Martin Todorov
 * @author Alex Oreshkevich
 * @author Pablo Tirado
 */
@IntegrationTest
public class TrashControllerUndeleteTest
        extends MavenRestAssuredBaseTest
{


    @Inject
    private MavenRepositoryFactory mavenRepositoryFactory;


    @BeforeAll
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
        repositories.add(createRepositoryMock(STORAGE0, "tcut-tuaftfr-releases", Maven2LayoutProvider.ALIAS));

        return repositories;
    }

    @Test
    public void testUndeleteArtifactFromTrashForRepository()
            throws Exception
    {
        // Test resources initialization start:
        String repositoryId = "tcut-tuaftfr-releases";
        String repositoryBaseDir = getRepositoryBasedir(STORAGE0, repositoryId).getAbsolutePath();

        MutableMavenRepositoryConfiguration mavenRepositoryConfiguration = new MutableMavenRepositoryConfiguration();
        mavenRepositoryConfiguration.setIndexingEnabled(true);

        MutableRepository repositoryWithTrash = mavenRepositoryFactory.createRepository(repositoryId);
        repositoryWithTrash.setTrashEnabled(true);
        repositoryWithTrash.setRepositoryConfiguration(mavenRepositoryConfiguration);

        createRepository(STORAGE0, repositoryWithTrash);

        generateArtifact(repositoryBaseDir,
                         "org.carlspring.strongbox.undelete:test-artifact-undelete",
                         new String[]{ "1.0" });

        client.delete(STORAGE0,
                      repositoryId,
                      "org/carlspring/strongbox/undelete/test-artifact-undelete/1.0/test-artifact-undelete-1.0.jar");

        // Test resources initialization end.

        assertFalse(indexContainsArtifact(STORAGE0,
                                          repositoryId,
                                          "+g:org.carlspring.strongbox.undelete " +
                                          "+a:test-artifact-undelete " +
                                          "+v:1.0 " +
                                          "+p:jar"));

        String url = getContextBaseUrl() + "/api/trash/" + STORAGE0 + "/" + repositoryId +
                     "/org/carlspring/strongbox/undelete/test-artifact-undelete/1.0/test-artifact-undelete-1.0.jar";

        given().header(HttpHeaders.ACCEPT, MediaType.TEXT_PLAIN_VALUE)
               .when()
               .post(url)
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value())
               .body(equalTo("The trash for '" + STORAGE0 + ":" + repositoryId + "' was restored successfully."));

        final Path artifactFileRestoredFromTrash = Paths.get(repositoryBaseDir + "/" +
                                                             "org/carlspring/strongbox/undelete/test-artifact-undelete/1.0/" +
                                                             "test-artifact-undelete-1.0.jar");

        final Path artifactFileInTrash = Paths.get(repositoryBaseDir + "/.trash/" +
                                                   "org/carlspring/strongbox/undelete/test-artifact-undelete/1.0/" +
                                                   "test-artifact-undelete-1.0.jar");

        assertFalse(Files.exists(artifactFileInTrash),
                    "Failed to undelete trash for repository '" + repositoryId + "'!");
        assertTrue(Files.exists(artifactFileRestoredFromTrash),
                   "Failed to undelete trash for repository '" + repositoryId + "'!");

    }

}
