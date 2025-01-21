import json
import requests

API_KEY = '83243ecdb4fc430eba8f1e074acacdbb'
BASE_URL = 'https://newsapi.org/v2/top-headlines'


def fetch_news(country='us', category='business'):
    params = {
        'country': country,
        'category': category,
        'apiKey': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        news_data = response.json()
        return news_data.get('articles', [])
    else:
        print(f"Failed to retrieve news: {response.status_code}")
        return []


def lambda_handler(event, context):
    articles = fetch_news()

    if articles:
        result = []
        for article in articles:
            result.append({
                "Title": article.get('title', 'No Title'),
                "Description": article.get('description', 'No Description'),
                "URL": article.get('url', 'No URL')
            })
        # Return the result so that Lambda response contains the data
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    else:
        return {
            'statusCode': 404,
            'body': json.dumps({"message": "No articles to display"})
        }


