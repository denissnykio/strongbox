package org.carlspring.strongbox.controllers.configuration;

import org.carlspring.strongbox.config.IntegrationTest;
import org.carlspring.strongbox.configuration.MutableConfiguration;
import org.carlspring.strongbox.rest.common.RestAssuredBaseTest;
import org.carlspring.strongbox.storage.MutableStorage;

import javax.inject.Inject;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.http.MediaType;
import static io.restassured.module.mockmvc.RestAssuredMockMvc.given;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * @author Pablo Tirado
 */
@IntegrationTest
@Execution(CONCURRENT)
public class StrongboxConfigurationControllerTestIT
        extends RestAssuredBaseTest
{

    @Inject
    private ObjectMapper objectMapper;


    @Override
    @BeforeEach
    public void init()
            throws Exception
    {
        super.init();
    }
    
    @Test
    public void testGetAndSetConfiguration()
            throws IOException
    {
        MutableConfiguration configuration = getConfigurationFromRemote();

        MutableStorage storage = new MutableStorage("storage3");

        configuration.addStorage(storage);

        String url = getContextBaseUrl() + "/api/configuration/strongbox";

        given().contentType(MediaType.APPLICATION_JSON_VALUE)
               .accept(MediaType.APPLICATION_JSON_VALUE)
               .body(objectMapper.writeValueAsString(configuration))
               .when()
               .put(url)
               .then()
               .statusCode(200);

        final MutableConfiguration c = getConfigurationFromRemote();

        assertNotNull(c.getStorage("storage3"), "Failed to create storage3!");
    }

    public MutableConfiguration getConfigurationFromRemote()
            throws IOException
    {
        String url = getContextBaseUrl() + "/api/configuration/strongbox";

        return objectMapper.readValue(given().accept(MediaType.APPLICATION_JSON_VALUE)
                                             .when()
                                             .get(url)
                                             .asString(),
                                      MutableConfiguration.class);
    }

}
