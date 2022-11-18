package awsservicestutorial.client;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;

public class AwsS3Client {

    private final AmazonS3 amazonS3;

    public AwsS3Client(final AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }

    public void uploadObject(final String bucketName, final Path path) throws IOException {

        isBucketExists(bucketName);

        final String filename = path.getFileName().toString();

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(URLConnection.guessContentTypeFromName(filename));
        metadata.setSSEAlgorithm("AES256");
        metadata.setContentLength(Files.size(path));

        try(final InputStream stream = Files.newInputStream(path)){
            amazonS3.putObject(bucketName, filename, stream, metadata);
        }
    }

    private void isBucketExists(final String bucketName) {
        if(!amazonS3.doesBucketExist(bucketName)) {
            throw new IllegalStateException(String.format("Bucket %s does not exist", bucketName));
        }
    }

}
