package org.carlspring.strongbox.controllers.layout.npm;

import org.carlspring.strongbox.artifact.coordinates.NpmArtifactCoordinates;
import org.carlspring.strongbox.config.IntegrationTest;
import org.carlspring.strongbox.domain.ArtifactEntry;
import org.carlspring.strongbox.domain.RemoteArtifactEntry;
import org.carlspring.strongbox.providers.layout.NpmLayoutProvider;
import org.carlspring.strongbox.rest.common.NpmRestAssuredBaseTest;
import org.carlspring.strongbox.services.ArtifactEntryService;
import org.carlspring.strongbox.storage.repository.MutableRepository;
import org.carlspring.strongbox.storage.repository.NpmRepositoryFactory;
import org.carlspring.strongbox.storage.repository.RepositoryTypeEnum;

import javax.inject.Inject;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import static io.restassured.module.mockmvc.RestAssuredMockMvc.given;
import static org.junit.jupiter.api.Assertions.*;

@IntegrationTest
public class NpmArtifactControllerTestIT
        extends NpmRestAssuredBaseTest
{

    @Inject
    private NpmRepositoryFactory npmRepositoryFactory;

    @Inject
    @Qualifier("contextBaseUrl")
    private String contextBaseUrl;

    @Inject
    private ArtifactEntryService artifactEntryService;

    @BeforeAll
    public static void cleanUp()
        throws Exception
    {
        cleanUp(getRepositoriesToClean());
    }

    public static Set<MutableRepository> getRepositoriesToClean()
    {
        Set<MutableRepository> repositories = new LinkedHashSet<>();
        repositories.add(createRepositoryMock(STORAGE0, "nactit-travp-npmjs-proxy", NpmLayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "nactit-travgwp-npmjs-proxy", NpmLayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "nactit-travgwp-group", NpmLayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "nactit-tvavp-npmjs-proxy", NpmLayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "nactit-tsavp-npmjs-proxy", NpmLayoutProvider.ALIAS));

        return repositories;
    }

    @Override
    @BeforeEach
    public void init()
        throws Exception
    {
        super.init();
    }

    /**
     * Note: This test requires an Internet connection.
     *
     * @throws Exception
     */
    @Test
    public void testResolveArtifactViaProxy()
            throws Exception
    {
        String repositoryId = "nactit-travp-npmjs-proxy";

        createProxyRepository(STORAGE0, repositoryId, "https://registry.npmjs.org/");

        // https://registry.npmjs.org/compression/-/compression-1.7.2.tgz
        String artifactPath = "/storages/" + STORAGE0 + "/" + repositoryId + "/" +
                              "compression/-/compression-1.7.1.tgz";

        resolveArtifact(artifactPath);
    }

    /**
     * Note: This test requires an Internet connection.
     *
     * @throws Exception
     */
    @Test
    public void testResolveArtifactViaGroupWithProxy()
            throws Exception
    {
        String repositoryIdProxy = "nactit-travgwp-npmjs-proxy";
        String repositoryIdGroup = "nactit-travgwp-group";

        createProxyRepository(STORAGE0, repositoryIdProxy, "https://registry.npmjs.org/");

        MutableRepository npmGroupReposiotry = npmRepositoryFactory.createRepository(repositoryIdGroup);
        npmGroupReposiotry.setType(RepositoryTypeEnum.GROUP.getType());
        npmGroupReposiotry.setGroupRepositories(Sets.newHashSet(STORAGE0 + ":" + repositoryIdProxy));

        createRepository(STORAGE0, npmGroupReposiotry);

        // https://registry.npmjs.org/compression/-/compression-1.7.2.tgz
        String artifactPath = "/storages/" + STORAGE0 + "/" + repositoryIdGroup + "/" +
                              "compression/-/compression-1.7.2.tgz";

        resolveArtifact(artifactPath);
    }

    @Test
    public void testViewArtifactViaProxy()
            throws Exception
    {
        String repositoryId = "nactit-tvavp-npmjs-proxy";

        createProxyRepository(STORAGE0, repositoryId, "https://registry.npmjs.org/");

        NpmArtifactCoordinates c = NpmArtifactCoordinates.of("react", "16.5.0");

        given().header("User-Agent", "npm/*")
               .when()
               .get(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/" + c.getId())
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value())
               .and()
               .body("name", CoreMatchers.equalTo("react"))
               .body("versions.size()", Matchers.greaterThan(0));

        ArtifactEntry artifactEntry = artifactEntryService.findOneArtifact(STORAGE0, repositoryId, c.toPath());

        assertNotNull(artifactEntry);
        assertTrue(artifactEntry instanceof RemoteArtifactEntry);
        assertFalse(((RemoteArtifactEntry)artifactEntry).getIsCached());
    }

    @Test
    public void testSearchArtifactViaProxy()
            throws Exception
    {
        String repositoryId = "nactit-tsavp-npmjs-proxy";

        createProxyRepository(STORAGE0, repositoryId, "https://registry.npmjs.org/");

        given().header("User-Agent", "npm/*")
               .when()
               .get(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/-/v1/search?text=reston&size=10")
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value())
               .and()
               .body("objects.package.name", CoreMatchers.hasItem("Reston"));

        ArtifactEntry artifactEntry = artifactEntryService.findOneArtifact(STORAGE0,
                                                                           repositoryId,
                                                                           "Reston/Reston/0.2.0/Reston-0.2.0.tgz");

        assertNotNull(artifactEntry);
        assertTrue(artifactEntry instanceof RemoteArtifactEntry);
        assertFalse(((RemoteArtifactEntry)artifactEntry).getIsCached());
    }

}
