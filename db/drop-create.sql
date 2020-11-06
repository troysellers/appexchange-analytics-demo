DROP TABLE IF EXISTS subscriber_snapshot;
DROP TABLE IF EXISTS package_usage_summary;
DROP TABLE IF EXISTS package_usage_log;

CREATE TABLE IF NOT EXISTS subscriber_snapshot  (
	id SERIAL,
	date DATE,
	organization_id TEXT,
	organization_name TEXT,
	organization_status TEXT,
	organization_edition TEXT,
	package_id TEXT,
	package_version_id TEXT,
	managed_package_namespace TEXT,
	custom_entity TEXT,
	count INTEGER
);

CREATE TABLE IF NOT EXISTS package_usage_summary (
	id SERIAL,
	month TEXT,
	organization_id TEXT,
	package_id TEXT,
	managed_package_namespace TEXT,
	custom_entity TEXT,
	custom_entity_type TEXT,
	user_id_token TEXT,
	user_type TEXT,
	num_creates INTEGER,
	num_reads INTEGER,
	num_updates INTEGER,
	num_deletes INTEGER,
	num_views INTEGER		
);

CREATE TABLE IF NOT EXISTS package_usage_log (
	id SERIAL,
	timestamp_derived TIMESTAMP,
	log_record_type	TEXT, 
	request_id	TEXT,
	organization_id	TEXT,
	organization_name TEXT,
	organization_status	TEXT,
	organization_edition TEXT,
	organization_country_code TEXT,	
	organization_language_locale TEXT,
	organization_time_zone TEXT,
	organization_instance TEXT,
	organization_type TEXT,
	cloned_from_organization_id TEXT,
	user_id_token TEXT,
	user_type TEXT,
	url	TEXT,
	package_id TEXT,
	package_version_id TEXT,
	managed_package_namespace TEXT,
	custom_entity TEXT,
	custom_entity_type TEXT,
	operation_type	TEXT,
	operation_count	TEXT,
	request_status	TEXT,
	referrer_uri	TEXT,
	session_key	TEXT,
	login_key	TEXT,
	user_agent	TEXT,
	user_country_code	TEXT,
	user_time_zone	TEXT,
	api_type	TEXT,
	api_version	TEXT,
	rows_processed	INTEGER,
	request_size	INTEGER,
	response_size	INTEGER,
	http_method	TEXT,
	http_status_code	SMALLINT,
	num_fields	TEXT,
	app_name	TEXT,
	page_app_name	TEXT,
	page_context	TEXT,
	ui_event_source	TEXT,
	ui_event_type	TEXT,
	ui_event_sequence_num	TEXT,
	target_ui_element	TEXT,
	parent_ui_element	TEXT,
	page_url	TEXT,
	prevpage_url TEXT,
	class_name TEXT,
	method_name TEXT,
	event TEXT,
	event_subscriber TEXT,
	event_count INTEGER
);

GRANT ALL PRIVILEGES on ALL TABLES IN SCHEMA public TO troybo;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO troybo;

	

