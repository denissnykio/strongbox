package org.carlspring.strongbox.controllers.layout.raw;

import org.carlspring.strongbox.config.IntegrationTest;
import org.carlspring.strongbox.providers.layout.RawLayoutProvider;
import org.carlspring.strongbox.rest.common.RawRestAssuredBaseTest;
import org.carlspring.strongbox.storage.repository.MutableRepository;
import org.carlspring.strongbox.storage.repository.RawRepositoryFactory;
import org.carlspring.strongbox.storage.repository.RepositoryPolicyEnum;
import org.carlspring.strongbox.storage.repository.RepositoryTypeEnum;

import javax.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * @author Martin Todorov
 */
@IntegrationTest
@Execution(CONCURRENT)
public class RawArtifactControllerTestIT
        extends RawRestAssuredBaseTest
{

    private static final String REPOSITORY_RELEASES = "ractit-raw-releases";

    private static final String REPOSITORY_PROXY = "ractit-raw-proxy";

    private static final String REPOSITORY_GROUP = "ractit-raw-group";

    @Inject
    RawRepositoryFactory rawRepositoryFactory;


    @BeforeAll
    public static void cleanUp()
            throws Exception
    {
        cleanUp(getRepositoriesToClean());
    }

    public static Set<MutableRepository> getRepositoriesToClean()
    {
        Set<MutableRepository> repositories = new LinkedHashSet<>();
        repositories.add(createRepositoryMock(STORAGE0, REPOSITORY_RELEASES, RawLayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, REPOSITORY_PROXY, RawLayoutProvider.ALIAS));
        repositories.add(createRepositoryMock(STORAGE0, REPOSITORY_GROUP, RawLayoutProvider.ALIAS));

        return repositories;
    }

    @Override
    @BeforeEach
    public void init()
            throws Exception
    {
        // Notes:
        // This tests resources currently don't need per test method isolation.
        // In case the requirements change, this test will need to be refactored.
        super.init();

        MutableRepository repository1 = rawRepositoryFactory.createRepository(REPOSITORY_RELEASES);
        repository1.setPolicy(RepositoryPolicyEnum.RELEASE.getPolicy());

        createRepository(STORAGE0, repository1);

        //noinspection ResultOfMethodCallIgnored
        Files.createDirectories(Paths.get(TEST_RESOURCES));

        createProxyRepository(STORAGE0,
                              REPOSITORY_PROXY,
                              "http://slackbuilds.org/slackbuilds/14.2/");
        // Required for http://www-eu.apache.org/dist/maven/pom/apache-19-source-release.zip

        MutableRepository repository2 = rawRepositoryFactory.createRepository(REPOSITORY_GROUP);
        repository2.setType(RepositoryTypeEnum.GROUP.getType());
        repository2.setGroupRepositories(Sets.newHashSet(STORAGE0 + ":" + REPOSITORY_PROXY));

        createRepository(STORAGE0, repository2);
        // Required for apache-19-source-release.zip
    }

    /**
     * Note: This test requires an internet connection.
     *
     * @throws Exception
     */
    @Test
    public void testResolveArtifactViaProxy()
            throws Exception
    {
        String artifactPath = "/storages/" + STORAGE0 + "/" + REPOSITORY_PROXY +
                              "/system/alien.tar.gz";

        resolveArtifact(artifactPath);
    }

    /**
     * Note: This test requires an internet connection.
     *
     * @throws Exception
     */
    @Test
    public void testResolveArtifactViaGroupWithProxy()
            throws Exception
    {
        String artifactPath = "/storages/" + STORAGE0 + "/" + REPOSITORY_GROUP +
                              "/academic/arpack.tar.gz";
        // http://slackbuilds.org/slackbuilds/14.2/academic/arpack.tar.gz
        resolveArtifact(artifactPath);
    }

}
