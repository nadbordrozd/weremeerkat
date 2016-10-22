.separator ","


CREATE TABLE documents_meta (
    document_id INT,
    source_id INT,
    publisher_id INT,
    publish_time TIMESTAMP
);
.import ../raw/documents_meta.csv documents_meta

CREATE TABLE page_views_sample (
    uuid VARCHAR(20),
    document_id INT,
    timestamp TIMESTAMP,
    platform INT,
    geo_location VARCHAR(20),
    traffic_source INT
);
.import ../raw/page_views_sample.csv page_views_sample

CREATE TABLE clicks_train (
    display_id INT,
    ad_id INT,
    clicked INT
);
.import ../raw/clicks_train.csv clicks_train

CREATE TABLE clicks_test (
    display_id INT,
    ad_id INT
);
.import ../raw/clicks_test.csv clicks_test

CREATE TABLE events (
    display_id INT,
    uuid VARCHAR(20),
    document_id INT,
    timestamp TIMESTAMP,
    platform INT,
    geo_location VARCHAR(20)
);
.import ../raw/events.csv events

CREATE TABLE promoted_content (
    ad_id INT,
    document_id INT,
    campaign_id INT,
    advertiser_id INT
);
.import ../raw/promoted_content.csv promoted_content

CREATE TABLE documents_topics (
    document_id INT,
    topic_id INT,
    confidence_level REAL
);
.import ../raw/documents_topics.csv documents_topics

CREATE TABLE documents_entities (
    document_id INT,
    entity_id VARCHAR(40),
    confidence_level REAL
);
.import ../raw/documents_entities.csv documents_entities

CREATE TABLE documents_categories (
    document_id INT,
    category_id INT,
    confidence_level REAL
);
.import ../raw/documents_categories.csv documents_categories

CREATE TABLE page_views (
    uuid VARCHAR(20),
    document_id INT,
    timestamp TIMESTAMP,
    platform INT,
    geo_location VARCHAR(20),
    traffic_source INT
);
.import ../raw/page_views.csv page_views
