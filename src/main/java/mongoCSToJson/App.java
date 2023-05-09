package mongoCSToJson;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;

public class App extends Thread {
	private static AtomicInteger counter = new AtomicInteger(0);

	@Override
	public void run() {
		while (true) {
			int cur = counter.get();
			counter.set(0);
			System.out.println("Published "+cur+" records");
			try {
				Thread.sleep(5);

			} catch (InterruptedException e) {
				e.printStackTrace();

			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, ExecutionException, InterruptedException {
		final String topic = "mongo-streaming-topic";
		MessagePublisher pub = null;

		String mongoUri = System.getenv("MONGODB_URI");
		String mongoDbList = System.getenv("MONGO_DB_LIST");
		String saCredsFile = System.getenv("GOOGLE_SA_CREDENTIALS");
		String projectId = System.getenv("GOOGLE_SA_PROJECT");

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ssxxx").withZone(ZoneOffset.UTC);

		JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
				.objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
				.dateTimeConverter((value, writer) -> {
					ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
					writer.writeString(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime));
				}).build();

		ServiceAccountCredentials sourceCredentials = ServiceAccountCredentials
				.fromStream(new FileInputStream(saCredsFile));

		pub = new MessagePublisher(sourceCredentials, projectId, topic);

		App appTimer = new App();
		appTimer.start();

		MongoClient mc = MongoClients.create(mongoUri);
		for (String db : mc.listDatabaseNames()) {
			System.out.println("Database -> " + db);
		}

		MongoDatabase db = mc.getDatabase(mongoDbList);
		MongoCursor<ChangeStreamDocument<Document>> cursor = db.watch().fullDocument(FullDocument.UPDATE_LOOKUP)
				.iterator();
		ChangeStreamDocument<Document> n = null;

		while (true) {
			n = cursor.next();
			//System.out.println(n.toString());
			BsonDocument idDoc = n.getDocumentKey();
			Instant wt = null;
			if ( n.getWallTime() != null )
				wt = new Date(n.getWallTime().getValue()).toInstant();

			if (n.getOperationType() == OperationType.DELETE) {
			} else if (n.getOperationType() == OperationType.DROP) {
				System.out.println("Handling drop of " + n.getNamespace().getCollectionName());
			} else if (n.getOperationType() == OperationType.DROP_DATABASE) {
				System.err.println("Unhandled operation for ns " + n.getNamespace().toString());
			} else {
				System.out.println("id -> "+n.getNamespace().getCollectionName()+
						"/"+idDoc.getObjectId("_id").getValue().toString() + " -> "+wt.toString());
			}

			int cur = counter.addAndGet(1);
		}
	}
}
