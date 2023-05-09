package mongoCSToJson;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

public class MessagePublisher {
	private String projectId;
	private TopicName topicName;
	private Publisher publisher;

	public MessagePublisher(GoogleCredentials credentials, String projectId, String topicId) throws IOException {
		this.projectId = projectId;
		this.topicName = TopicName.of(projectId, topicId);
		this.publisher = Publisher.newBuilder(topicName)
				.setCredentialsProvider(FixedCredentialsProvider.create(credentials))
				.build();
	}

	/* a cleanup method could use this
	if (publisher != null) {
		// When finished with the publisher, shutdown to free up resources.
		publisher.shutdown();
		publisher.awaitTermination(1, TimeUnit.MINUTES);
	}
	*/

	public void publishMessage(String message)
			throws IOException, ExecutionException, InterruptedException {
		ByteString data = ByteString.copyFromUtf8(message);
		PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

		// Once published, returns a server-assigned message id (unique within the topic)
		ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
		String messageId = messageIdFuture.get();
		System.out.println("Published message ID: " + messageId);
	}
}
