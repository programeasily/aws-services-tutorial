package awsservicestutorial;

import awsservicestutorial.client.AwsS3Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(SpringExtension.class)
class AwsServicesTutorialApplicationTests {
	private static final String BUCKET_NAME = "test-bucket";
	@Container
	public final static LocalStackContainer LOCAL_STACK_CONTAINER =
			new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.12.16"))
					.withServices(LocalStackContainer.Service.S3).withEnv("DEFAULT_REGION", "us-east-1");
	private AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
			.withEndpointConfiguration(LOCAL_STACK_CONTAINER.getEndpointConfiguration(LocalStackContainer.Service.S3))
			.withCredentials(LOCAL_STACK_CONTAINER.getDefaultCredentialsProvider()).build();

	private AwsS3Client awsS3Client;

	@BeforeEach
	public void beforeEach() throws IOException, InterruptedException {
		LOCAL_STACK_CONTAINER.execInContainer("awslocal", "s3", "mb", "s3://" + BUCKET_NAME);
		awsS3Client = new AwsS3Client(amazonS3);
	}

	@AfterEach
	public void afterEach() throws IOException, InterruptedException {
		LOCAL_STACK_CONTAINER.execInContainer("awslocal", "s3", "rb", "s3://" + BUCKET_NAME, "--force");
	}

	/**
	 * First assert whether localstack is running!
	 */
	@Test
	public void test_isLocalstackRunning() {
		Assertions.assertTrue(LOCAL_STACK_CONTAINER.isRunning());
	}

	@Test
	public void test_uploadObjectSuccess() throws URISyntaxException, IOException {

		awsS3Client.uploadObject(BUCKET_NAME, createPath());
		Assertions.assertTrue(amazonS3.doesObjectExist(BUCKET_NAME, "sample.csv"));
	}

	@Test
	public void tes_uploadObjectError() throws IOException, InterruptedException {

		LOCAL_STACK_CONTAINER.execInContainer("awslocal", "s3", "rb", "s3://" + BUCKET_NAME, "--force");
		Assertions.assertThrows(IllegalStateException.class, () -> awsS3Client.uploadObject(BUCKET_NAME, createPath()));
	}

	private Path createPath() throws URISyntaxException {
		return Optional.ofNullable(ClassLoader.getSystemResource("sample.csv").toURI())
				.map(Paths::get)
				.orElseThrow(IllegalArgumentException::new);
	}

}
