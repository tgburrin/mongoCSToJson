package mongoCSToJson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MongoMessage {
	public String id;
	public String eventDt;
	public String mongoDt;
	public String type;
	public String operation;
	public String object;

	@JsonProperty("event_dt")
	public String getEventDt() {
		return eventDt;
	}

	@JsonProperty("mongo_dt")
	public String getMongoDt() {
		return mongoDt;
	}
}

/*
https://stackoverflow.com/questions/75142178/nullable-date-in-avro-schema-for-google-pub-sub
https://issuetracker.google.com/issues/242757468

create table if not exists test_ds.mongo_stream_event (
id string,
event_dt timestamp not null,
mongo_dt timestamp,
type string not null,
operation string not null,
object json
);
*/