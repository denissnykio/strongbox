package org.carlspring.strongbox.controllers.configuration.security.authentication;

import org.carlspring.strongbox.authentication.ConfigurableProviderManager;
import org.carlspring.strongbox.authentication.api.AuthenticationItem;
import org.carlspring.strongbox.authentication.api.AuthenticationItems;
import org.carlspring.strongbox.config.IntegrationTest;
import org.carlspring.strongbox.rest.common.RestAssuredBaseTest;

import javax.inject.Inject;
import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import static io.restassured.module.mockmvc.RestAssuredMockMvc.given;
import static org.carlspring.strongbox.CustomMatchers.equalByToString;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;

/**
 * @author Przemyslaw Fusik
 * @author Pablo Tirado
 * @author sbespalov
 */
@IntegrationTest
public class AuthenticatorsConfigControllerTestIT
        extends RestAssuredBaseTest
{

    @Inject
    private ConfigurableProviderManager configurableProviderManager;

    @Override
    @BeforeEach
    public void init()
            throws Exception
    {
        super.init();
        setContextBaseUrl(getContextBaseUrl() + "/api/configuration");
    }

    @AfterEach
    public void afterEveryTest()
            throws IOException
    {
        configurableProviderManager.reload();

    }

    @Test
    public void registryShouldReturnExpectedInitialArray()
    {
        assertInitialAuthenticationItems();
    }

    private void assertInitialAuthenticationItems()
    {
        given().when()
               .get(getContextBaseUrl() + "/authenticators/")
               .peek()
               .then()
               .body("authenticationItemList[0].name",
                     equalByToString("authenticationProviderFirst"))
               .body("authenticationItemList[0].order",
                     equalByToString("0"))
               .body("authenticationItemList[0].enabled",
                     equalByToString("true"))
               .body("authenticationItemList[1].name",
                     equalByToString("authenticationProviderSecond"))
               .body("authenticationItemList[1].order",
                     equalByToString("1"))
               .body("authenticationItemList[1].enabled",
                     equalByToString("true"))
               .body("authenticationItemList[2].name",
                     equalByToString("authenticationProviderThird"))
               .body("authenticationItemList[2].order",
                     equalByToString("2"))
               .body("authenticationItemList[2].enabled",
                     equalByToString("false"))
               .body("authenticationItemList[3].name",
                     equalByToString("passwordAuthenticationProvider"))
               .body("authenticationItemList[3].order",
                     equalByToString("5"))
               .body("authenticationItemList.size()", is(8))
               .statusCode(HttpStatus.OK.value());
    }

    @Test
    public void registryShouldBeReloadable()
    {
        assertInitialAuthenticationItems();

        given().header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
               .when()
               .put(getContextBaseUrl()
                    + "/authenticators/reorder/authenticationProviderFirst/authenticationProviderSecond")
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value())
               .body(containsString(AuthenticatorsConfigController.SUCCESSFUL_REORDER));

        // Confirm they are re-ordered
        given().when()
               .get(getContextBaseUrl() + "/authenticators/")
               .peek()
               .then()
               .body("authenticationItemList[0].name",
                     equalByToString("authenticationProviderSecond"))
               .body("authenticationItemList[0].order",
                     equalByToString("0"))
               .body("authenticationItemList[1].name",
                     equalByToString("authenticationProviderFirst"))
               .body("authenticationItemList[1].order",
                     equalByToString("1"))
               .statusCode(HttpStatus.OK.value());
    }

    @Test
    public void authenticationItemCanBeEnabled()
    {
        assertInitialAuthenticationItems();

        AuthenticationItem authenticationItem = new AuthenticationItem("authenticationProviderThird",
                                                                       AuthenticationProvider.class.getSimpleName());
        authenticationItem.setEnabled(true);
        authenticationItem.setOrder(2);

        AuthenticationItems authenticationItems = new AuthenticationItems();
        authenticationItems.getAuthenticationItemList().add(authenticationItem);

        given().header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
               .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
               .body(authenticationItems)
               .when()
               .put(getContextBaseUrl() + "/authenticators/")
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value())
               .body(containsString(AuthenticatorsConfigController.SUCCESSFUL_UPDATE));

        given().when()
               .get(getContextBaseUrl() + "/authenticators/")
               .peek()
               .then()
               .body("authenticationItemList[2].name",
                     equalByToString("authenticationProviderThird"))
               .body("authenticationItemList[2].enabled",
                     equalByToString("true"))
               .statusCode(HttpStatus.OK.value());
    }

    static class TestAuthenticationProvider
            implements AuthenticationProvider
    {

        @Override
        public Authentication authenticate(Authentication authentication)
                throws AuthenticationException
        {
            return null;
        }

        @Override
        public boolean supports(Class<?> authentication)
        {
            return true;
        }
    }

}
