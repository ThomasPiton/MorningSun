"""
News Example for MorningPy
"""
from morningpy.api.news import get_headline_news

def run():
    # Get headline news
    news = get_headline_news(
        market="Spain",
        news="economy",
        edition="Central Europe"
    )
    
    print("Headline News:")
    print(news.head())
    
    return "Correctly extracted !"
    
if __name__ == "__main__":
    print(run())
