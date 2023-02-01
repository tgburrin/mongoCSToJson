package mongoCSToJson;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;

public class App extends Thread {
	private static AtomicInteger totalCount = new AtomicInteger(0);
	private static AtomicInteger counter = new AtomicInteger(0);
	private static Instant lastWrite = Instant.now();
	private static Connection dbc = null;

	private static final int commitLimit = 5000;

	@Override
	public void run() {
		while (true) {
			synchronized (lastWrite) {
				if (ChronoUnit.MILLIS.between(lastWrite, Instant.now()) > 250 && counter.get() > 0) {
					try {
						dbc.commit();
						int cur = counter.get();
						counter.set(0);
						lastWrite = Instant.now();
						int tc = totalCount.addAndGet(cur);
						System.out.println("Committed "+cur+" records based on time: "+tc);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			try {
				Thread.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		String mongoUri = "mongodb://tgb-01-26.tgburrin.net,tgb-01-27.tgburrin.net,tgb-01-28.tgburrin.net/";
		String postgresUri = "jdbc:postgresql://forseti.tgburrin.net:5432/dwstage";

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ssxxx").withZone(ZoneOffset.UTC);

		JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
				.objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
				.dateTimeConverter((value, writer) -> {
					ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
					writer.writeString(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime));
				}).build();

		String adddocsql = "insert into landing.mongo_raw";
		adddocsql += "(id, event_dt, type, operation, object)";
		adddocsql += " values ";
		adddocsql += "(('\\x'||?::text)::bytea, ?::timestamptz, ?::text, ?::text, ?::jsonb) ";

		PreparedStatement ps = null;

		try {
			Properties props = new Properties();
			props.setProperty("user", "mongorep");
			props.setProperty("password", "mypassword");

			Class.forName("org.postgresql.Driver");
			dbc = DriverManager.getConnection(postgresUri, props);
			dbc.setAutoCommit(false);
			ps = dbc.prepareStatement(adddocsql);
		} catch (SQLException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		App appTimer = new App();
		appTimer.start();

		try (MongoClient mc = MongoClients.create(mongoUri)) {
			for (String db : mc.listDatabaseNames()) {
				System.out.println("Database -> " + db);
			}

			MongoDatabase db = mc.getDatabase("test");
			MongoCursor<ChangeStreamDocument<Document>> cursor = db.watch().fullDocument(FullDocument.UPDATE_LOOKUP)
					.iterator();
			ChangeStreamDocument<Document> n = null;
			while (true) {
				n = cursor.next();
				// System.out.println(n.toString());
				BsonDocument idDoc = n.getDocumentKey();
				Instant wt = new Date(n.getWallTime().getValue()).toInstant();

				if (n.getOperationType() == OperationType.DELETE) {
					// System.out.println("id -> "+n.getNamespace().getCollectionName()+
					// "/"+idDoc.getObjectId("_id").getValue().toString() + " -> "+wt.toString());
					ps.setString(1, idDoc.getObjectId("_id").getValue().toString());
					ps.setString(2, wt.toString());
					ps.setString(3, n.getNamespace().getCollectionName());
					ps.setString(4, n.getOperationTypeString());

					ps.setString(5, n.getDocumentKey().toJson(settings));
					ps.execute();
					// System.out.println(n.getOperationTypeString()+" ->
					// "+n.getDocumentKey().toJson(settings));
				} else if (n.getOperationType() == OperationType.DROP) {
					System.out.println("Handling drop of " + n.getNamespace().getCollectionName());
					String sql = "with base as (\n" + "        select \n" + "				id, \n"
							+ "				type, \n" + "				'drop' as operation, \n"
							+ "				jsonb_set('{}'::jsonb, '{_id}', object #> '{_id}') \n" + "from \n"
							+ "				landing.mongo_raw \n" + "where \n" + "				type = ?::text \n"
							+ "and operation not in ('drop') \n" + "and valid_to_dt = 'infinity'\n"
							+ ") insert into landing.mongo_raw (event_dt, id, type, operation, object) select ?::timestamptz, * from base";
					PreparedStatement dropStatement = dbc.prepareStatement(sql);
					dropStatement.setString(1, n.getNamespace().getCollectionName());
					dropStatement.setString(2, wt.toString());
					dropStatement.execute();
				} else if (n.getOperationType() == OperationType.DROP_DATABASE) {
					System.err.println("Unhandled operation for ns " + n.getNamespace().toString());
				} else {
					// System.out.println("id -> "+n.getNamespace().getCollectionName()+
					// "/"+idDoc.getObjectId("_id").getValue().toString() + " -> "+wt.toString());
					ps.setString(1, idDoc.getObjectId("_id").getValue().toString());
					ps.setString(2, wt.toString());
					ps.setString(3, n.getNamespace().getCollectionName());
					ps.setString(4, n.getOperationTypeString());

					ps.setString(5, n.getFullDocument().toJson(settings));
					ps.execute();
					// System.out.println(n.getOperationTypeString()+" ->
					// "+n.getFullDocument().toJson(settings));
				}

				synchronized (lastWrite) {
					lastWrite = Instant.now();
				}
				int cur = counter.addAndGet(1);
				if (cur > commitLimit) {
					counter.set(0);
					dbc.commit();
					int tc = totalCount.addAndGet(cur);
					System.out.println("Committed "+cur+" records based on count: "+tc);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
