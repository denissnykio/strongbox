package org.carlspring.strongbox.config;

import org.carlspring.strongbox.MockedRemoteRepositoriesHeartbeatConfig;
import org.carlspring.strongbox.app.StrongboxSpringBootApplication;
import org.carlspring.strongbox.data.CacheManagerTestExecutionListener;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.restassured.module.mockmvc.RestAssuredMockMvc;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.security.test.context.support.WithUserDetails;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;


/**
 * Helper meta annotation for all rest-assured based tests. Specifies tests that
 * require web server and remote HTTP protocol.
 *
 * @author Alex Oreshkevich
 * @author Przemyslaw Fusik
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(classes = { StrongboxSpringBootApplication.class,
                            MockedRemoteRepositoriesHeartbeatConfig.class,
                            IntegrationTest.TestConfig.class,
                            RestAssuredConfig.class })
@WebAppConfiguration("classpath:")
@WithUserDetails(value = "admin")
@ActiveProfiles(profiles = "test")
@ContextConfiguration(locations = "classpath:/ldapServerApplicationContext.xml")
@TestPropertySource(locations = "classpath:/org/carlspring/strongbox/authentication/api/impl/ldap/lapt.properties")
@TestExecutionListeners(listeners = { CacheManagerTestExecutionListener.class }, mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
@Execution(CONCURRENT)
public @interface IntegrationTest
{

    @Configuration
    class TestConfig
    {

        @EventListener
        void handleContextRefreshedEvent(ContextRefreshedEvent event)
        {
            ApplicationContext applicationContext = event.getApplicationContext();
            if (applicationContext instanceof WebApplicationContext)
            {
                RestAssuredMockMvc.webAppContextSetup((WebApplicationContext) applicationContext);
            }

        }

        @EventListener
        void handleContextStoppedEvent(ContextStoppedEvent event)
        {
            ApplicationContext applicationContext = event.getApplicationContext();
            if (applicationContext instanceof WebApplicationContext)
            {
                RestAssuredMockMvc.reset();
            }
        }
    }
}