package org.carlspring.strongbox.controllers.layout.npm;

import org.carlspring.strongbox.artifact.coordinates.NpmArtifactCoordinates;
import org.carlspring.strongbox.artifact.generator.NpmPackageGenerator;
import org.carlspring.strongbox.config.IntegrationTest;
import org.carlspring.strongbox.providers.layout.NpmLayoutProvider;
import org.carlspring.strongbox.rest.common.NpmRestAssuredBaseTest;
import org.carlspring.strongbox.storage.repository.MutableRepository;
import org.carlspring.strongbox.storage.repository.NpmRepositoryFactory;
import org.carlspring.strongbox.storage.repository.RepositoryPolicyEnum;

import javax.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import static io.restassured.module.mockmvc.RestAssuredMockMvc.given;
import static org.hamcrest.CoreMatchers.equalTo;

@IntegrationTest
public class NpmArtifactControllerTest
        extends NpmRestAssuredBaseTest
{

    @Inject
    private NpmRepositoryFactory npmRepositoryFactory;

    @Inject
    @Qualifier("contextBaseUrl")
    private String contextBaseUrl;

    ;


    @BeforeAll
    public static void cleanUp()
        throws Exception
    {
        cleanUp(getRepositoriesToClean());
    }

    public static Set<MutableRepository> getRepositoriesToClean()
    {
        Set<MutableRepository> repositories = new LinkedHashSet<>();
        repositories.add(createRepositoryMock(STORAGE0, "nact-tvp-releases", NpmLayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, "nact-tpcf-releases", NpmLayoutProvider.ALIAS));

        return repositories;
    }

    @Override
    @BeforeEach
    public void init()
        throws Exception
    {
        super.init();
    }

    @Test
    public void testViewPackage()
        throws Exception
    {
        String repositoryId = "nact-tvp-releases";

        MutableRepository repository = npmRepositoryFactory.createRepository(repositoryId);
        repository.setPolicy(RepositoryPolicyEnum.RELEASE.getPolicy());

        createRepository(STORAGE0, repository);

        NpmArtifactCoordinates coordinates = NpmArtifactCoordinates.of("@carlspring/npm-test-view", "1.0.0");

        String repositoryBasedir = getRepositoryBasedir(STORAGE0, repositoryId).getAbsolutePath();
        NpmPackageGenerator packageGenerator = new NpmPackageGenerator(repositoryBasedir);

        Path publishJsonPath = packageGenerator.of(coordinates).buildPublishJson();

        byte[] publishJsonContent = Files.readAllBytes(publishJsonPath);

        //Publish
        given().header("User-Agent", "npm/*")
               .header("Content-Type", "application/json")
               .body(publishJsonContent)
               .when()
               .put(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/" + coordinates.getId())
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value());

        // View OK
        given().header("User-Agent", "npm/*")
               .header("Content-Type", "application/json")
               .when()
               .get(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/" +
                    coordinates.getId() + "/" + coordinates.getVersion())
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value());
        
        // View 404
        given().header("User-Agent", "npm/*")
               .header("Content-Type", "application/json")
               .when()
               .get(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/" +
                    coordinates.getId() + "/1.0.1")
               .peek()
               .then()
               .statusCode(HttpStatus.NOT_FOUND.value());
    }
    
    @Test
    public void testPackageCommonFlow()
        throws Exception
    {
        String repositoryId = "nact-tpcf-releases";

        MutableRepository repository = npmRepositoryFactory.createRepository(repositoryId);
        repository.setPolicy(RepositoryPolicyEnum.RELEASE.getPolicy());

        createRepository(STORAGE0, repository);

        NpmArtifactCoordinates coordinates = NpmArtifactCoordinates.of("@carlspring/npm-test-release", "1.0.0");

        String repositoryBasedir = getRepositoryBasedir(STORAGE0, repositoryId).getAbsolutePath();
        NpmPackageGenerator packageGenerator = new NpmPackageGenerator(repositoryBasedir);

        Path publishJsonPath = packageGenerator.of(coordinates).buildPublishJson();
        Path packagePath = packageGenerator.getPackagePath();

        byte[] publishJsonContent = Files.readAllBytes(publishJsonPath);

        //Publish
        given().header("User-Agent", "npm/*")
               .header("Content-Type", "application/json")
               .body(publishJsonContent)
               .when()
               .put(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/" + coordinates.getId())
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value());

        //View
        given().header("User-Agent", "npm/*")
               .header("Content-Type", "application/json")
               .when()
               .get(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/" + coordinates.getId())
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value());
        
        //Download
        given().header("User-Agent", "npm/*")
               .header("Content-Type", "application/json")
               .when()
               .get(contextBaseUrl + "/storages/" + STORAGE0 + "/" + repositoryId + "/" + coordinates.toResource())
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value())
               .assertThat()
               .header("Content-Length", equalTo(String.valueOf(Files.size(packagePath))));
    }

}
