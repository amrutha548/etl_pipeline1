import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from textblob import TextBlob

# AWS RDS Database credentials
db_host = 'newsapi-db.chus66qw2fhd.ap-south-1.rds.amazonaws.com'
db_name = 'task_pipeline'  # Ensure the database name is correct
db_user = 'newspostgres'
db_password = '*athira1*'  # Ensure the password is correct

# Create SQLAlchemy engine
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}')

# Fetch news data
def fetch_news_data():
    query = "SELECT * FROM news_articles"  # Updated query
    return pd.read_sql(query, engine)

# Function to perform sentiment analysis on text
def analyze_sentiment(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    if polarity > 0:
        sentiment = "Positive"
    elif polarity < 0:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"
    return polarity, sentiment

# Streamlit app UI
st.title("Latest News Headlines")
st.write("Here are the latest headlines fetched from the database with sentiment analysis:")

try:
    # Retrieve data from the database
    news_df = fetch_news_data()

    # Check if data exists
    if not news_df.empty:
        # Apply sentiment analysis to each title in the news dataframe
        news_df[['polarity', 'sentiment']] = news_df['title'].apply(lambda x: pd.Series(analyze_sentiment(x)))

        # Display the table with titles, polarity, and sentiment scores
        st.dataframe(news_df[['title', 'polarity', 'sentiment']])

        # Display individual articles with their sentiment scores
        for index, row in news_df.iterrows():
            st.subheader(row['title'])  # Display the title
            st.write(f"Sentiment: {row['sentiment']} (Score: {row['polarity']:.2f})")  # Display sentiment and score
            st.write(row['description'])  # Display the description
            st.write(f"[Read more]({row['url']})")  # Link to the article
            st.write("---")
    else:
        st.write("No news articles found.")
except Exception as e:
    st.error(f"An error occurred: {e}")
finally:
    # Dispose of the engine connection
    engine.dispose()

