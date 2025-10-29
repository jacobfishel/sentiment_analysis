"""
Phase 1.1: Collect Historical Prices, S&P 500 Indices, and News Data

This script performs three main tasks:
(a) Download historical daily prices for stocks and S&P 500 index
(b) Download full news articles from URLs via web scraping
(c) Merge datasets into all_news.csv

Input files (must be provided):
- analyst_ratings.csv
- headlines.csv

Output files:
- historical_prices.csv
- all_news.csv
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from time import sleep
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataset_schema import historical_prices_schema, all_news_schema
from utils import load_and_validate_csv


def download_historical_prices(symbols, start_date="2009-01-01", end_date="2020-12-31"):
    """
    Download historical daily prices for stocks and S&P 500 index.
    
    Args:
        symbols: List of stock symbols
        start_date: Start date for historical data
        end_date: End date for historical data
    
    Returns:
        DataFrame with schema: date, symbol, open, high, low, close, volume
    """
    print("Downloading historical prices...")
    
    # Add S&P 500 index (use ^GSPC as symbol, but save as 's&p')
    all_symbols = list(set(symbols)) + ['^GSPC']
    
    all_data = []
    
    for symbol in all_symbols:
        print(f"  Downloading {symbol}...")
        try:
            # Download data using yfinance
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date)
            
            if df.empty:
                print(f"    Warning: No data found for {symbol}")
                continue
            
            # Reset index to get date as column
            df = df.reset_index()
            
            # Rename columns to match schema
            df = df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',  # yfinance Close is already adjusted close
                'Volume': 'volume'
            })
            
            # Add symbol column (use 's&p' for S&P 500)
            df['symbol'] = 's&p' if symbol == '^GSPC' else symbol
            
            # Select only required columns
            df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            
            all_data.append(df)
            
        except Exception as e:
            print(f"    Error downloading {symbol}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No historical price data could be downloaded")
    
    # Combine all data
    prices_df = pd.concat(all_data, ignore_index=True)
    
    # Sort by date and symbol
    prices_df = prices_df.sort_values(['date', 'symbol']).reset_index(drop=True)
    
    print(f"Downloaded {len(prices_df)} price records for {len(all_symbols)} symbols")
    
    return prices_df


def scrape_article(url, publisher):
    """
    Scrape full article content from a URL.
    
    Args:
        url: URL to scrape
        publisher: Publisher name (used to determine parsing strategy)
    
    Returns:
        Article text or empty string if scraping fails
    """
    # Set up headers to mimic a real browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }
    
    try:
        # Send GET request with retry logic
        for attempt in range(3):
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                break
            sleep(1)  # Wait before retrying
        
        if response.status_code != 200:
            return ""
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove unwanted elements (scripts, styles, nav, ads)
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            element.decompose()
        
        # Try different strategies based on publisher
        article_text = ""
        
        # Strategy 1: Look for article tag
        article = soup.find('article')
        if article:
            article_text = article.get_text(separator=' ', strip=True)
        
        # Strategy 2: Look for common article body classes
        if not article_text:
            for class_name in ['article-body', 'article-content', 'entry-content', 
                              'post-content', 'content-body']:
                article_div = soup.find('div', class_=lambda x: x and class_name in x.lower() if x else False)
                if article_div:
                    article_text = article_div.get_text(separator=' ', strip=True)
                    break
        
        # Strategy 3: Look for paragraph tags within main content
        if not article_text:
            paragraphs = soup.find_all('p')
            if paragraphs:
                article_text = ' '.join([p.get_text(strip=True) for p in paragraphs])
        
        # Clean up text
        article_text = ' '.join(article_text.split())  # Normalize whitespace
        
        return article_text[:10000]  # Limit to 10000 characters
        
    except Exception as e:
        print(f"    Error scraping {url}: {e}")
        return ""


def download_articles(news_df, delay=1):
    """
    Download full articles for all news items.
    
    Args:
        news_df: DataFrame with columns: id, headline, URL, publisher, date, symbol
        delay: Delay in seconds between requests (to respect rate limits)
    
    Returns:
        DataFrame with added 'article' column
    """
    print("Downloading full articles...")
    
    articles = []
    total = len(news_df)
    
    for idx, row in news_df.iterrows():
        if (idx + 1) % 10 == 0:
            print(f"  Progress: {idx + 1}/{total}")
        
        url = row['URL']
        publisher = row['publisher']
        
        # Check if URL is accessible (skip blocked domains like gurufocus)
        if 'gurufocus' in url.lower():
            print(f"    Skipping gurufocus URL (blocked): {url}")
            articles.append("")
        else:
            article_text = scrape_article(url, publisher)
            articles.append(article_text)
            
            # Respect rate limits
            sleep(delay)
    
    news_df['article'] = articles
    
    # Count successful scrapes
    success_count = sum(1 for a in articles if a)
    print(f"Successfully scraped {success_count}/{total} articles")
    
    return news_df


def merge_datasets(analyst_ratings_path, headlines_path):
    """
    Merge analyst_ratings.csv and headlines.csv, add article column, and create all_news.csv.
    
    Args:
        analyst_ratings_path: Path to analyst_ratings.csv
        headlines_path: Path to headlines.csv
    
    Returns:
        DataFrame with schema: id, headline, URL, article, publisher, date, symbol
    """
    print("Merging datasets...")
    
    # Load datasets
    analyst_ratings_df = pd.read_csv(analyst_ratings_path)
    headlines_df = pd.read_csv(headlines_path)
    
    print(f"  Loaded {len(analyst_ratings_df)} analyst ratings")
    print(f"  Loaded {len(headlines_df)} headlines")
    
    # Ensure both have the same columns
    required_cols = ['id', 'headline', 'URL', 'publisher', 'date', 'symbol']
    
    # Concatenate datasets
    all_news_df = pd.concat([
        analyst_ratings_df[required_cols],
        headlines_df[required_cols]
    ], ignore_index=True)
    
    # Reset IDs to be sequential
    all_news_df['id'] = range(1, len(all_news_df) + 1)
    
    # Convert date to datetime
    all_news_df['date'] = pd.to_datetime(all_news_df['date'])
    
    # Sort by date
    all_news_df = all_news_df.sort_values('date').reset_index(drop=True)
    
    print(f"  Merged dataset has {len(all_news_df)} total news items")
    
    # Download articles (this may take a while)
    # Note: In practice, you might want to batch this or use multiprocessing
    all_news_df = download_articles(all_news_df, delay=1)
    
    # Reorder columns to match schema
    all_news_df = all_news_df[['id', 'headline', 'URL', 'article', 'publisher', 'date', 'symbol']]
    
    return all_news_df


def main():
    """
    Main execution function for Phase 1.1
    """
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    datasets_dir = os.path.join(base_dir, 'datasets')
    
    analyst_ratings_path = os.path.join(base_dir, 'analyst_ratings.csv')
    headlines_path = os.path.join(base_dir, 'headlines.csv')
    
    historical_prices_path = os.path.join(datasets_dir, 'historical_prices.csv')
    all_news_path = os.path.join(datasets_dir, 'all_news.csv')
    
    # Create datasets directory if it doesn't exist
    os.makedirs(datasets_dir, exist_ok=True)
    
    print("="*60)
    print("Phase 1.1: Data Collection")
    print("="*60)
    
    # Check if input files exist
    if not os.path.exists(analyst_ratings_path):
        print(f"ERROR: {analyst_ratings_path} not found")
        print("Please provide the analyst_ratings.csv file in the Assignment_2 directory")
        return
    
    if not os.path.exists(headlines_path):
        print(f"ERROR: {headlines_path} not found")
        print("Please provide the headlines.csv file in the Assignment_2 directory")
        return
    
    # Task (a): Download historical prices
    print("\n" + "="*60)
    print("Task (a): Downloading Historical Prices")
    print("="*60)
    
    # First, get unique symbols from news datasets
    analyst_ratings_df = pd.read_csv(analyst_ratings_path)
    headlines_df = pd.read_csv(headlines_path)
    
    symbols = list(set(
        analyst_ratings_df['symbol'].dropna().unique().tolist() +
        headlines_df['symbol'].dropna().unique().tolist()
    ))
    
    print(f"Found {len(symbols)} unique stock symbols")
    
    # Download historical prices
    prices_df = download_historical_prices(symbols)
    
    # Validate schema
    print("Validating historical_prices schema...")
    try:
        historical_prices_schema.validate(prices_df, lazy=True)
        print("  Schema validation passed")
    except Exception as e:
        print(f"  Schema validation failed: {e}")
    
    # Save to CSV
    prices_df.to_csv(historical_prices_path, index=False)
    print(f"Saved to {historical_prices_path}")
    
    # Task (b) & (c): Download articles and merge datasets
    print("\n" + "="*60)
    print("Task (b) & (c): Scraping Articles and Merging Datasets")
    print("="*60)
    
    all_news_df = merge_datasets(analyst_ratings_path, headlines_path)
    
    # Validate schema
    print("Validating all_news schema...")
    try:
        all_news_schema.validate(all_news_df, lazy=True)
        print("  Schema validation passed")
    except Exception as e:
        print(f"  Schema validation failed: {e}")
    
    # Save to CSV
    all_news_df.to_csv(all_news_path, index=False)
    print(f"Saved to {all_news_path}")
    
    print("\n" + "="*60)
    print("Phase 1.1 Complete!")
    print("="*60)
    print(f"Generated files:")
    print(f"  - {historical_prices_path}")
    print(f"  - {all_news_path}")


if __name__ == "__main__":
    main()
