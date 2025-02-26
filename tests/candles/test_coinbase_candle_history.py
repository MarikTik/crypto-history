import pytest
import aiohttp
from datetime import datetime, timezone
from src.coinbase_candle_history import CoinbaseCandleHistory


@pytest.mark.asyncio
async def test_fetch_timeframe_valid():
    """Test that fetching historical data returns valid OHLCV candles."""
    async with aiohttp.ClientSession() as session:
        start_time = datetime(2024, 2, 10, 12, 0, tzinfo=timezone.utc)  # Adjust as needed
        end_time = datetime(2024, 2, 10, 13, 0, tzinfo=timezone.utc)  # 1-hour window
        
        result = await CoinbaseCandleHistory.fetch_timeframe(
            session, "BTC-USDT", start_time, end_time, granularity=60
        )
        assert "symbol" in result
        assert result["symbol"] == "BTC-USDT"
        assert "data" in result
        assert isinstance(result["data"], list)
        assert len(result["data"]) > 0  # Should return at least one candle
        assert len(result["data"][0]) == 6  # OHLCV format
        expected_timestamp = 1707569940   
        assert result["data"][0][0] == expected_timestamp  

            


@pytest.mark.asyncio
async def test_fetch_timeframe_invalid_symbol():
    """Test that an invalid symbol returns no data and logs an error."""
    async with aiohttp.ClientSession() as session:
        start_time = datetime(2024, 2, 10, 12, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 2, 10, 13, 0, tzinfo=timezone.utc)
        
        result = await CoinbaseCandleHistory.fetch_timeframe(
            session, "INVALID-COIN", start_time, end_time, granularity=60
        )
        assert result is None  # Should return nothing for invalid symbols


@pytest.mark.asyncio
async def test_fetch_continuous_mode():
    """Test that the fetch function runs continuously when no end_date is provided."""
    async for result in CoinbaseCandleHistory.fetch(["BTC-USDT"], start_date="2024-02-10", end_date=None, granularity=60):
        assert "symbol" in result
        assert result["symbol"] == "BTC-USDT"
        assert "data" in result
        assert isinstance(result["data"], list)
        assert len(result["data"]) > 0  # Should continuously return data

        break  # Stop after the first yield

@pytest.mark.asyncio
async def test_fetch_multiple_coins():
    """Test that fetching multiple coins returns valid responses for all."""
    symbols = ["BTC-USDT", "ETH-USDT"]
    async for result in CoinbaseCandleHistory.fetch(symbols, start_date="2024-02-10", end_date="2024-02-11", granularity=60):
        assert "symbol" in result
        assert result["symbol"] in symbols  # Must be one of the requested coins
        assert "data" in result
        assert isinstance(result["data"], list)
        assert len(result["data"]) > 0  # Should return at least one candle

        break  # Stop after first batch

@pytest.mark.asyncio
async def test_fetch_invalid_granularity():
    """Test that an invalid granularity is handled properly."""
    async with aiohttp.ClientSession() as session:
        start_time = datetime(2024, 2, 10, 12, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 2, 10, 13, 0, tzinfo=timezone.utc)
        
        result = await CoinbaseCandleHistory.fetch_timeframe(
            session, "BTC-USDT", start_time, end_time, granularity=45  # Invalid granularity
        )

        assert result is None  # Coinbase should reject unsupported granularities

@pytest.mark.asyncio
async def test_fetch_edge_case_timeframe():
    """Test fetching data for a very small timeframe (e.g., 1 minute)."""
    async with aiohttp.ClientSession() as session:
        start_time = datetime(2024, 2, 10, 12, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 2, 10, 12, 1, tzinfo=timezone.utc)  # 1-minute window
        
        result = await CoinbaseCandleHistory.fetch_timeframe(
            session, "BTC-USDT", start_time, end_time, granularity=60
        )

        assert "data" in result
        assert isinstance(result["data"], list)
        assert len(result["data"]) > 0  # Ensure we got at least 1 candle
        
        # Check that all returned candles are within the expected timeframe
        for candle in result["data"]:
            candle_timestamp = candle[0]  # First element is time in epoch
            assert start_time.timestamp() <= candle_timestamp <= end_time.timestamp()



@pytest.mark.asyncio
async def test_fetch_404():
    """Test handling of 404 response from Coinbase."""
    async with aiohttp.ClientSession() as session:
        start_time = datetime(2024, 2, 10, 12, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 2, 10, 13, 0, tzinfo=timezone.utc)
        
        result = await CoinbaseCandleHistory.fetch_timeframe(
            session, "NONEXISTENT-COIN", start_time, end_time, granularity=60
        )

        assert result is None  # 404 should not return any data
