import pytest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from classy_skkkrapey.my_scrapers.scraper_ibizaspotlight_mongo_fixed import (
    IbizaSpotlightScraper, 
    ScraperConfig
)
from classy_skkkrapey.database.mongodb_setup import MongoDBSetup

# Sample event data matching MongoDB schema
SAMPLE_EVENT = {
    "url": "https://www.ibiza-spotlight.com/night/events/test-event",
    "scrapedAt": datetime.now(timezone.utc),
    "extractionMethod": "html_parsing",
    "title": "Test Event",
    "location": {
        "venue": "Test Venue",
        "address": "Test Address",
        "city": "Ibiza",
        "country": "Spain"
    },
    "dateTime": {
        "start": datetime(2025, 6, 1, 22, 0),
        "end": datetime(2025, 6, 2, 6, 0),
        "displayText": "Sat 1 Jun 2025"
    },
    "lineUp": [
        {"name": "DJ Test", "headliner": True}
    ],
    "description": "This is a test event description.",
    "_quality": {
        "scores": {
            "title": 0.9,
            "location": 0.8,
            "dateTime": 0.95,
            "lineUp": 0.85,
            "ticketInfo": 0.0
        },
        "overall": 0.85,
        "lastCalculated": datetime.now(timezone.utc)
    }
}

@pytest.fixture
def test_config():
    """Fixture for scraper configuration"""
    return ScraperConfig(
        url="https://www.ibiza-spotlight.com/night/events",
        mongodb_uri="mongodb://localhost:27017/test_db"
    )

def test_mongodb_local_connection(test_config):
    """Test MongoDB local connection"""
    # Setup
    os.environ["MONGO_URI"] = "mongodb://localhost:27017/"
    setup = MongoDBSetup()
    
    # Test connection
    assert setup.connect() is True
    assert setup.client is not None
    setup.close()

@patch.dict(os.environ, {"MONGO_URI": "mongodb+srv://user:pass@cluster.mongodb.net/test"})
def test_mongodb_atlas_connection():
    """Test MongoDB Atlas connection"""
    setup = MongoDBSetup()
    
    # Test connection
    with patch('pymongo.MongoClient') as mock_client:
        mock_client.return_value.admin.command.return_value = True
        assert setup.connect() is True
        assert setup.client is not None
    setup.close()

def test_event_insertion_schema(test_config):
    """Test event insertion with schema validation"""
    scraper = IbizaSpotlightScraper(test_config)
    
    # Mock MongoDB connection
    mock_db = MagicMock()
    scraper.db = mock_db
    
    # Test insertion
    scraper.save_to_mongodb(SAMPLE_EVENT)
    
    # Verify document structure matches schema
    inserted_doc = mock_db.events.update_one.call_args[0][1]['$set']
    assert inserted_doc["url"] == SAMPLE_EVENT["url"]
    assert inserted_doc["title"] == SAMPLE_EVENT["title"]
    assert "venue" in inserted_doc["location"]
    assert "start" in inserted_doc["dateTime"]
    assert "_quality" in inserted_doc

def test_connection_failure_handling(test_config):
    """Test error handling for connection failures"""
    # Force connection failure during initialization
    with patch('classy_skkkrapey.my_scrapers.scraper_ibizaspotlight_mongo_fixed.MongoDBSetup.connect', return_value=False):
        with pytest.raises(ConnectionError):
            scraper = IbizaSpotlightScraper(test_config)

def test_connection_closing(test_config):
    """Test proper connection closure"""
    scraper = IbizaSpotlightScraper(test_config)
    
    with patch.object(scraper.mongo_setup.client, 'close') as mock_close:
        scraper.mongo_setup.close()
        mock_close.assert_called_once()

@patch('classy_skkkrapey.my_scrapers.scraper_ibizaspotlight_mongo_fixed.requests.Session.get')
def test_full_scraping_workflow(mock_get, test_config):
    """Test full scraping workflow with MongoDB integration"""
    # Mock HTTP response
    mock_response = MagicMock()
    mock_response.text = """
        <html>
            <h1 class="eventTitle">Real Event</h1>
            <div class="venue-name">Real Venue</div>
            <div class="event-date">2025-06-15</div>
            <div class="event-time">22:00</div>
            <div class="lineup">
                <div class="lineup-artist">
                    <span class="artist-name">Real DJ</span>
                </div>
            </div>
            <div class="event-description">Real description</div>
        </html>
    """
    mock_get.return_value = mock_response
    
    # Create scraper
    scraper = IbizaSpotlightScraper(test_config)
    
    # Mock MongoDB methods
    scraper.save_to_mongodb = MagicMock()
    scraper.crawl_calendar = MagicMock(return_value=["https://test-event.com"])
    
    # Run scraper
    scraper.run()
    
    # Verify MongoDB interaction
    assert scraper.save_to_mongodb.call_count == 1
    saved_event = scraper.save_to_mongodb.call_args[0][0]
    assert saved_event["title"] == "Real Event"
    assert saved_event["location"]["venue"] == "Real Venue"
    # Check that quality data exists and has expected structure
    assert "_quality" in saved_event
    assert "scores" in saved_event["_quality"]
    assert "overall" in saved_event["_quality"]["scores"]

def test_required_fields_validation(test_config):
    """Test validation of required fields in document"""
    scraper = IbizaSpotlightScraper(test_config)
    mock_db = MagicMock()
    scraper.db = mock_db
    
    # Create invalid event missing required fields
    invalid_event = SAMPLE_EVENT.copy()
    del invalid_event["url"]
    del invalid_event["scrapedAt"]
    
    # Test insertion
    scraper.save_to_mongodb(invalid_event)
    
    # Verify error handling
    assert mock_db.events.update_one.call_count == 0