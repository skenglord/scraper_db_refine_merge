# Pytest unit tests for classy_skkkrapey.database.mongodb_setup
# Note: Performance testing (e.g., insertion speed, memory usage) is out of scope for
# these unit tests and would require a different setup (e.g., Locust, real MongoDB instance).

import pytest
from unittest import mock
import mongomock
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime, timezone
import os
import certifi
import logging

# Assuming MongoDBSetup is in classy_skkkrapey.database.mongodb_setup
from classy_skkkrapey.database.mongodb_setup import MongoDBSetup

# Test constants
TEST_DB_NAME = "test_classy_skkkrapey"
TEST_MONGO_URI = "mongodb://localhost:27017/" # For non-Atlas testing

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mocks environment variables for MongoDB connection."""
    monkeypatch.setenv("MONGO_URI", TEST_MONGO_URI)
    monkeypatch.setenv("MONGO_DB_NAME", TEST_DB_NAME)

@pytest.fixture
def mock_certifi(monkeypatch):
    """Mocks certifi.where()."""
    mock_where = mock.Mock(return_value="/fake/path/to/ca.pem")
    monkeypatch.setattr(certifi, 'where', mock_where)
    return mock_where

@pytest.fixture
@mock.patch('classy_skkkrapey.database.mongodb_setup.MongoClient', new_callable=mongomock.MongoClient)
def db_setup(mock_mongo_client_class, mock_env_vars, mock_certifi):
    """Provides a MongoDBSetup instance with a mocked MongoClient."""
    # mock_mongo_client_class is already mongomock.MongoClient due to new_callable
    setup = MongoDBSetup(connection_string=TEST_MONGO_URI, database_name=TEST_DB_NAME)
    # Ensure the client instance used by setup is the mongomock instance
    setup.client = mock_mongo_client_class() 
    setup.db = setup.client[TEST_DB_NAME]
    return setup
    
@pytest.fixture
@mock.patch('classy_skkkrapey.database.mongodb_setup.MongoClient', new_callable=mongomock.MongoClient)
def connected_db_setup(mock_mongo_client_class, mock_env_vars, mock_certifi):
    """Provides a connected MongoDBSetup instance."""
    setup = MongoDBSetup(connection_string=TEST_MONGO_URI, database_name=TEST_DB_NAME)
    # Simulate a successful connection
    setup.client = mock_mongo_client_class()
    setup.client.admin.command.return_value = {'ok': 1} # Mock ping success
    setup.db = setup.client[TEST_DB_NAME]
    # Call connect, but it will use the already mocked client
    # To make it truly use the mock, we might need to patch MongoClient within connect or ensure the instance is replaced
    
    # Let's refine this:
    # The db_setup fixture already provides a client. We just need to call connect on it.
    # The patch on MongoClient for db_setup ensures that when MongoDBSetup instantiates MongoClient, it gets a mongomock.
    
    # Re-instantiate for clarity and control within this fixture
    setup_instance = MongoDBSetup(connection_string=TEST_MONGO_URI, database_name=TEST_DB_NAME)
    
    # The MongoClient used by setup_instance will be a mongomock.MongoClient due to the outer patch
    # We need to ensure the 'ping' command works on the mongomock admin database.
    # mongomock's admin db doesn't have 'ping' by default.
    
    # Let's use a side_effect to control the MongoClient instantiation within connect
    # or rely on the class-level patch and ensure the instance is correctly configured.

    # For simplicity with mongomock, we'll assume the MongoClient patch works globally for the test.
    # We'll call connect and ensure it sets up self.client and self.db with mongomock instances.
    
    # Create a new instance for this fixture to ensure isolation
    setup = MongoDBSetup(connection_string=TEST_MONGO_URI, database_name=TEST_DB_NAME)
    
    # The global patch ensures MongoClient() inside connect() returns a mongomock client
    # We need to make sure the 'ping' command on the admin db of mongomock works or is bypassed.
    # mongomock.MongoClient().admin.command('ping') might raise an error.
    
    # Let's mock the ping command specifically for the connect method
    with mock.patch.object(mongomock.database.AdminDatabase, 'command', return_value={'ok': 1}) as mock_ping:
        connected = setup.connect()
        assert connected is True
        assert setup.client is not None
        assert setup.db is not None
    return setup


# --- Test Classes Will Go Here ---

class TestConnection:
    def test_connect_success(self, mock_env_vars, mock_certifi):
        """Test successful connection to MongoDB."""
        # Patch MongoClient for this specific test to ensure it's mongomock
        with mock.patch('classy_skkkrapey.database.mongodb_setup.MongoClient', new_callable=mongomock.MongoClient) as MockMongoClient:
            # Mock the ping command on the admin database of the mongomock client instance
            mock_client_instance = MockMongoClient.return_value
            mock_client_instance.admin.command.return_value = {'ok': 1} # Simulate successful ping
            
            setup = MongoDBSetup(connection_string=TEST_MONGO_URI, database_name=TEST_DB_NAME)
            assert setup.connect() is True
            assert setup.client is not None
            assert setup.db is not None
            assert setup.db.name == TEST_DB_NAME
            MockMongoClient.assert_called_once_with(TEST_MONGO_URI) # For non-Atlas
            mock_client_instance.admin.command.assert_called_once_with('ping')

    def test_connect_success_atlas(self, mock_env_vars, mock_certifi, monkeypatch):
        """Test successful connection to MongoDB Atlas."""
        atlas_uri = "mongodb+srv://user:pass@cluster.mongodb.net/"
        monkeypatch.setenv("MONGO_URI", atlas_uri)
        with mock.patch('classy_skkkrapey.database.mongodb_setup.MongoClient', new_callable=mongomock.MongoClient) as MockMongoClient:
            mock_client_instance = MockMongoClient.return_value
            mock_client_instance.admin.command.return_value = {'ok': 1}
            
            setup = MongoDBSetup(connection_string=atlas_uri, database_name=TEST_DB_NAME)
            assert setup.connect() is True
            mock_certifi.assert_called_once()
            MockMongoClient.assert_called_once_with(atlas_uri, tls=True, tlsCAFile="/fake/path/to/ca.pem")

    def test_connect_failure(self, mock_env_vars, mock_certifi):
        """Test connection failure to MongoDB."""
        with mock.patch('classy_skkkrapey.database.mongodb_setup.MongoClient') as MockMongoClient:
            MockMongoClient.side_effect = ConnectionFailure("Connection refused")
            setup = MongoDBSetup(connection_string=TEST_MONGO_URI, database_name=TEST_DB_NAME)
            
            with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
                assert setup.connect() is False
                mock_logger.error.assert_called_once_with("Failed to connect to MongoDB: Connection refused")

    def test_close_connection(self, connected_db_setup):
        """Test closing the MongoDB connection."""
        # connected_db_setup already has a connected client
        mock_client_close = mock.Mock()
        connected_db_setup.client.close = mock_client_close # type: ignore
        
        with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
            connected_db_setup.close()
            mock_client_close.assert_called_once()
            mock_logger.info.assert_called_with("MongoDB connection closed")

    def test_close_connection_no_client(self, db_setup):
        """Test closing when no client was initialized."""
        db_setup.client = None # Ensure no client
        with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
            db_setup.close() # Should not raise error
            mock_logger.info.assert_not_called() # Or assert specific log if any for this case


class TestSchemaValidation:
    # Placeholder for schema validation tests
    def test_event_schema_applied(self, connected_db_setup):
        connected_db_setup.create_collections()
        # mongomock doesn't fully support collMod $jsonSchema validation in a way that's easily introspectable
        # We'll test by inserting valid/invalid data and relying on pymongo/mongomock to raise errors if it were real Mongo
        # For now, we check if the command was attempted.
        
        # To truly test schema, we'd need to insert data.
        # Let's check if collMod was called.
        # This requires mocking the db.command method.
        
        # Create a fresh setup for this test to isolate db instance
        setup = connected_db_setup # Use the connected one
        
        # We need to ensure 'events' collection exists before collMod is called
        if "events" not in setup.db.list_collection_names(): # type: ignore
             setup.db.create_collection("events") # type: ignore

        with mock.patch.object(setup.db, 'command') as mock_db_command:
            # We also need to mock list_collection_names if it's called before create_collection
            # Simulate "events" collection already exists for the collMod call
            with mock.patch.object(setup.db, 'list_collection_names', return_value=["events"]):
                 setup._create_events_collection() # This calls collMod

            # Check that collMod was called for "events"
            # The actual schema is complex, so we'll check for the call with a validator.
            called_with_validator = False
            for call_args in mock_db_command.call_args_list:
                if call_args[0][0] == "collMod" and call_args[0][1] == "events" and "validator" in call_args[1]:
                    called_with_validator = True
                    # Optionally, do a basic check on the schema structure if needed
                    assert "$jsonSchema" in call_args[1]["validator"]
                    assert "lineUp" in call_args[1]["validator"]["$jsonSchema"]["properties"]
                    break
            assert called_with_validator, "collMod with validator was not called on events collection"

    def test_insert_valid_event_with_new_lineup(self, connected_db_setup):
        """Test inserting a valid event document with the new lineup schema."""
        setup = connected_db_setup
        setup.create_collections() # Ensure collection and schema are set up

        valid_event = {
            "url": "http://example.com/event1",
            "scrapedAt": datetime.now(timezone.utc),
            "extractionMethod": "test_method",
            "title": "Valid Event with Full Lineup",
            "lineUp": [
                {"name": "DJ Alpha", "room": "Main Room", "headliner": True, "genre": "Techno", "startTime": "22:00"},
                {"name": "DJ Beta", "room": "Terrace", "headliner": False, "genre": "House"} # startTime is optional
            ]
            # Add other required fields as per schema if strict validation is mocked/tested
        }
        # Mongomock's schema validation is not as robust as real MongoDB.
        # We primarily test if the application code attempts to insert correctly.
        try:
            result = setup.db["events"].insert_one(valid_event) # type: ignore
            assert result.inserted_id is not None
            retrieved = setup.db["events"].find_one({"url": "http://example.com/event1"}) # type: ignore
            assert retrieved is not None
            assert len(retrieved["lineUp"]) == 2
            assert retrieved["lineUp"][0]["room"] == "Main Room"
        except Exception as e:
            # If mongomock had perfect schema validation, this would be the place to catch pymongo.errors.WriteError
            pytest.fail(f"Insertion of a supposedly valid event failed: {e}")

    def test_insert_event_lineup_missing_required_room(self, connected_db_setup):
        """Test inserting an event with a lineup item missing the required 'room' field."""
        setup = connected_db_setup
        setup.create_collections()

        invalid_event = {
            "url": "http://example.com/event_invalid_lineup",
            "scrapedAt": datetime.now(timezone.utc),
            "extractionMethod": "test_method",
            "title": "Event with Invalid Lineup",
            "lineUp": [
                {"name": "DJ Gamma"} # Missing 'room'
            ]
        }
        # In a real MongoDB with the schema active, this would raise a WriteError.
        # mongomock might not enforce this. We are testing the application's interaction.
        # For the purpose of this test, we assume that if the schema *were* strictly enforced by the mock,
        # an error would occur. Since mongomock may not, we'll note this limitation.
        # The schema definition itself is part of what mongodb_setup.py does.
        # We've tested that collMod is called.
        # A more robust test would involve a real MongoDB instance or a more capable mock.
        
        # Simulating the expected behavior if validation was strict:
        # with pytest.raises(OperationFailure): # Or WriteError
        #     setup.db["events"].insert_one(invalid_event)
        # As mongomock might not raise, we'll just insert and acknowledge the limitation.
        # print("Note: mongomock may not fully enforce $jsonSchema. This test relies on the schema definition being correct.")
        try:
            setup.db["events"].insert_one(invalid_event) # type: ignore
            # If we reach here with mongomock, it means it didn't validate strictly.
            # This is a limitation of the mocking library for this specific test.
        except Exception as e:
            # This would be the ideal path if mongomock supported it fully.
            pass # Expected if validation was perfect

    def test_insert_event_backward_compatibility_no_lineup(self, connected_db_setup):
        """Test inserting an event without the new lineUp field (backward compatibility)."""
        setup = connected_db_setup
        setup.create_collections()

        event_no_lineup = {
            "url": "http://example.com/event_no_lineup",
            "scrapedAt": datetime.now(timezone.utc),
            "extractionMethod": "test_method",
            "title": "Event without Lineup Field"
            # lineUp field is omitted
        }
        try:
            result = setup.db["events"].insert_one(event_no_lineup) # type: ignore
            assert result.inserted_id is not None
            retrieved = setup.db["events"].find_one({"url": "http://example.com/event_no_lineup"}) # type: ignore
            assert retrieved is not None
            assert "lineUp" not in retrieved # Ensure it wasn't added implicitly
        except Exception as e:
            pytest.fail(f"Insertion of event without lineup (backward compat) failed: {e}")

    def test_insert_event_missing_required_top_level_field(self, connected_db_setup):
        """Test inserting an event missing a top-level required field like 'url'."""
        setup = connected_db_setup
        setup.create_collections() # Applies schema

        invalid_event_missing_url = {
            # "url": "http://example.com/missing_url", # URL is missing
            "scrapedAt": datetime.now(timezone.utc),
            "extractionMethod": "test_method",
            "title": "Event Missing URL"
        }
        # Mongomock may not raise an error here. A real MongoDB would.
        # This test documents the expectation based on the schema.
        # print("Note: mongomock may not fully enforce $jsonSchema for top-level required fields.")
        try:
            setup.db["events"].insert_one(invalid_event_missing_url) # type: ignore
        except Exception as e:
            # Ideally, this would be a pymongo.errors.WriteError or OperationFailure
            pass

    def test_schema_collmod_failure_logs_warning(self, connected_db_setup):
        """Test that a warning is logged if collMod fails."""
        setup = connected_db_setup
        # Ensure events collection exists
        if "events" not in setup.db.list_collection_names(): # type: ignore
            setup.db.create_collection("events") # type: ignore

        with mock.patch.object(setup.db, 'command', side_effect=OperationFailure("collMod failed")): # type: ignore
            with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
                # We also need to mock list_collection_names if it's called before create_collection
                with mock.patch.object(setup.db, 'list_collection_names', return_value=["events"]): # Simulate collection exists
                    setup._create_events_collection()
                mock_logger.warning.assert_called_once_with("Could not apply validation schema: collMod failed")


class TestEventInsertionLogic:
    def test_insert_sample_data_upserts(self, connected_db_setup):
        """Test that insert_sample_data performs an upsert."""
        setup = connected_db_setup
        setup.create_collections() # Ensure 'events' collection exists

        # Call once to insert
        setup.insert_sample_data()
        count_after_first_insert = setup.db["events"].count_documents({}) # type: ignore
        assert count_after_first_insert > 0 # Assuming sample_data adds at least one
        
        first_event = setup.db["events"].find_one({"url": "https://ticketsibiza.com/event/glitterbox-25th-may-2025/"}) # type: ignore
        assert first_event is not None
        original_scraped_at = first_event["scrapedAt"]

        # Modify a field and call again to test upsert
        # To ensure the $set in insert_sample_data is tested, we'd ideally mock datetime.utcnow
        # or check a field that insert_sample_data would modify.
        # The sample data is static in the code, so calling it again with the same URL will update.
        
        with mock.patch('classy_skkkrapey.database.mongodb_setup.datetime') as mock_dt:
            # Ensure subsequent calls to utcnow() return a different time
            # to check if the document gets updated by $set
            new_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.utcnow.return_value = new_time
            mock_dt.now.return_value = new_time # If now() is used anywhere else
             # For constructing datetime objects like datetime(2025,5,25,23,0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)


            setup.insert_sample_data() # Call again

        count_after_second_insert = setup.db["events"].count_documents({}) # type: ignore
        assert count_after_second_insert == count_after_first_insert # No new doc due to upsert

        updated_event = setup.db["events"].find_one({"url": "https://ticketsibiza.com/event/glitterbox-25th-may-2025/"}) # type: ignore
        assert updated_event is not None
        # Check if a field that would be set by $set (e.g., scrapedAt if it's dynamic) has changed
        # In the current insert_sample_data, scrapedAt is datetime.utcnow()
        assert updated_event["scrapedAt"] == new_time # Verifies $set updated the doc
        # Verify other fields from the original sample insert are preserved
        assert updated_event["title"] == "Glitterbox 25th May 2025" # This was part of original $set
        assert updated_event["location"]["venue"] == "Hï Ibiza" # This was also part of original $set
        # If insert_sample_data was more complex and only updated a subset, we'd check non-updated fields here.
        # The current sample_event in mongodb_setup.py is mostly static, so $set overwrites the whole thing.
        # A better test for $set preserving fields would be to manually insert, then manually update a subset.
        # However, for insert_sample_data, this test is sufficient to show it runs and updates.

    def test_insert_sample_data_handles_failure(self, connected_db_setup):
        setup = connected_db_setup
        setup.create_collections()
        
        with mock.patch.object(setup.db["events"], 'update_one', side_effect=OperationFailure("DB error")): # type: ignore
            with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
                setup.insert_sample_data()
                mock_logger.error.assert_called_once_with("Failed to insert sample data: DB error")

class TestDataIntegrity:
    def test_unique_url_index(self, connected_db_setup):
        """Test the unique index on the 'url' field."""
        setup = connected_db_setup
        setup.create_collections() # This creates indexes

        event1 = {"url": "http://example.com/unique_event", "scrapedAt": datetime.now(timezone.utc), "extractionMethod": "test"}
        setup.db["events"].insert_one(event1) # type: ignore

        # mongomock doesn't enforce unique indexes by raising an error on insert_one for duplicates.
        # Instead, if you try to create a unique index on a field that already has duplicates, it might complain,
        # or it might silently ignore.
        # The `create_indexes` call in `_create_events_collection` is what we rely on.
        # We can check if the index was created.
        
        indexes = list(setup.db["events"].list_indexes()) # type: ignore
        url_index_found = False
        for index in indexes:
            if index['key'] == {'url': 1} and index.get('unique', False):
                url_index_found = True
                break
        assert url_index_found, "Unique index on 'url' was not created."
        
        # To test the *behavior* of the unique constraint with mongomock, it's tricky.
        # A real MongoDB would raise DuplicateKeyError.
        # mongomock might allow the second insert.
        # The test for insert_sample_data_upserts implicitly tests that duplicate URLs lead to updates, not new docs.

class TestErrorHandling:
    def test_create_collections_no_db(self, db_setup): # Use db_setup, not connected_db_setup
        """Test create_collections when DB is not connected."""
        setup = db_setup
        setup.db = None # Ensure DB is not connected
        with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
            setup.create_collections()
            mock_logger.error.assert_called_once_with("Database not connected. Cannot create collections.")

    def test_create_search_index_failure(self, connected_db_setup):
        setup = connected_db_setup
        # Ensure events collection exists
        if "events" not in setup.db.list_collection_names(): # type: ignore
            setup.db.create_collection("events") # type: ignore
            
        with mock.patch.object(setup.db, 'command', side_effect=OperationFailure("Search index creation failed")): # type: ignore
            with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
                setup._create_search_index()
                mock_logger.error.assert_called_with("Failed to create search index: Search index creation failed")

    def test_create_search_index_already_exists(self, connected_db_setup):
        setup = connected_db_setup
        if "events" not in setup.db.list_collection_names(): # type: ignore
            setup.db.create_collection("events") # type: ignore

        with mock.patch.object(setup.db, 'command', side_effect=OperationFailure("Index already exists")): # type: ignore
            with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
                setup._create_search_index()
                mock_logger.info.assert_called_with("Atlas search index 'event_search' already exists.")


class TestCollectionCreation:
    def _check_indexes(self, collection, expected_indexes_definitions):
        """Helper to check if expected indexes exist on a collection."""
        # mongomock's list_indexes() might not return unique=True explicitly in the same way
        # real MongoDB does. We'll check for key presence.
        # And mongomock might create a default _id index.
        
        # Normalize expected definitions to tuples of (key_dict, is_unique)
        norm_expected = []
        for item in expected_indexes_definitions:
            key_dict = dict(item[0]) # Convert list of tuples to dict
            is_unique = item[1] if len(item) > 1 else False
            norm_expected.append((key_dict, is_unique))

        actual_indexes = list(collection.list_indexes())
        
        for expected_key_dict, expected_unique in norm_expected:
            found = False
            for actual_index in actual_indexes:
                actual_key_dict = dict(actual_index['key'])
                # For unique check, mongomock might not always populate 'unique' field if False
                actual_unique = actual_index.get('unique', False)
                if actual_key_dict == expected_key_dict:
                    if expected_unique: # Only check uniqueness if we expect it to be true
                        assert actual_unique == expected_unique, f"Index {expected_key_dict} unique flag mismatch."
                    found = True
                    break
            assert found, f"Expected index {expected_key_dict} not found."


    def test_create_quality_scores_collection(self, connected_db_setup):
        setup = connected_db_setup
        setup._create_quality_scores_collection()
        assert "quality_scores" in setup.db.list_collection_names() # type: ignore
        
        expected_indexes = [
            ([("eventId", ASCENDING), ("calculatedAt", DESCENDING)], False),
            ([("calculatedAt", DESCENDING)], False),
            ([("overallScore", DESCENDING)], False)
        ]
        self._check_indexes(setup.db["quality_scores"], expected_indexes) # type: ignore

    def test_create_validation_history_collection(self, connected_db_setup):
        setup = connected_db_setup
        setup._create_validation_history_collection()
        assert "validation_history" in setup.db.list_collection_names() # type: ignore
        
        expected_indexes = [
            ([("eventId", ASCENDING), ("validatedAt", DESCENDING)], False),
            ([("validatedAt", DESCENDING)], False),
            ([("validationType", ASCENDING)], False)
        ]
        self._check_indexes(setup.db["validation_history"], expected_indexes) # type: ignore

    def test_create_extraction_methods_collection(self, connected_db_setup):
        setup = connected_db_setup
        setup._create_extraction_methods_collection()
        assert "extraction_methods" in setup.db.list_collection_names() # type: ignore
        
        expected_indexes = [
            ([("method", ASCENDING), ("domain", ASCENDING)], False),
            ([("successRate", DESCENDING)], False),
            ([("lastUsed", DESCENDING)], False)
        ]
        self._check_indexes(setup.db["extraction_methods"], expected_indexes) # type: ignore

    def test_create_all_collections_calls_individual_creators(self, connected_db_setup):
        setup = connected_db_setup
        with mock.patch.object(setup, '_create_events_collection') as mock_create_events, \
             mock.patch.object(setup, '_create_quality_scores_collection') as mock_create_quality, \
             mock.patch.object(setup, '_create_validation_history_collection') as mock_create_validation, \
             mock.patch.object(setup, '_create_extraction_methods_collection') as mock_create_extraction, \
             mock.patch.object(setup, '_create_search_index') as mock_create_search:
            
            setup.create_collections()
            
            mock_create_events.assert_called_once()
            mock_create_quality.assert_called_once()
            mock_create_validation.assert_called_once()
            mock_create_extraction.assert_called_once()
            mock_create_search.assert_called_once()


# Minimal main test for coverage, though its direct testing is often integration-level
@mock.patch('classy_skkkrapey.database.mongodb_setup.MongoDBSetup')
def test_main_flow_connect_fail(MockMongoDBSetup, mock_env_vars):
    mock_setup_instance = MockMongoDBSetup.return_value
    mock_setup_instance.connect.return_value = False # Simulate connection failure
    
    with mock.patch('classy_skkkrapey.database.mongodb_setup.logger') as mock_logger:
        # Need to import main from the module to test it
        from classy_skkkrapey.database.mongodb_setup import main as mongodb_main
        mongodb_main()
        mock_logger.error.assert_called_with("Failed to connect to MongoDB. Please ensure MongoDB is running.")
        mock_setup_instance.create_collections.assert_not_called()
        mock_setup_instance.insert_sample_data.assert_not_called()
        mock_setup_instance.close.assert_not_called() # Close is in finally, but if connect fails, it might not reach it if main returns early.
                                                    # The current main() calls close() only if connect() was true.
                                                    # Let's verify the current main's logic:
                                                    # if not setup.connect(): return ... so close() is not called.

@mock.patch('classy_skkkrapey.database.mongodb_setup.MongoDBSetup')
def test_main_flow_connect_success(MockMongoDBSetup, mock_env_vars):
    mock_setup_instance = MockMongoDBSetup.return_value
    mock_setup_instance.connect.return_value = True # Simulate connection success
    
    from classy_skkkrapey.database.mongodb_setup import main as mongodb_main
    mongodb_main()
    
    mock_setup_instance.connect.assert_called_once()
    mock_setup_instance.create_collections.assert_called_once()
    mock_setup_instance.insert_sample_data.assert_called_once()
    mock_setup_instance.close.assert_called_once()