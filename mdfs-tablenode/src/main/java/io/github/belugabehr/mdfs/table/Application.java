package io.github.belugabehr.mdfs.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@SpringBootApplication
@Configuration
@Component
public class Application implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

	@Override
	public void run(final String... args) throws Exception {
	}

//	@Bean
//	public LeaderInitiatorFactoryBean leaderInitiator(CuratorFramework client) {
//		LeaderInitiatorFactoryBean a = new LeaderInitiatorFactoryBean().setClient(client).setPath("/siTest/")
//				.setRole("master");
////		a.setLeaderEventPublisher(logger);
//		return a;
//	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
