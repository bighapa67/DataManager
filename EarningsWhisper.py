import pandas as pd
import datetime as dt
import requests
from bs4 import BeautifulSoup

def get_data():
    url = 'https://www.earningswhispers.com/calendar'
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    # Click "Show More" button to reveal all data
    show_more_button = soup.find('button', {'id': 'showMoreButton'})
    if show_more_button:
        show_more_url = 'https://www.earningswhispers.com' + show_more_button['onclick'].split("'")[1]
        show_more_response = requests.get(show_more_url)
        soup = BeautifulSoup(show_more_response.content, 'html.parser')

    # Extract the data from the table
    table = soup.find('table', {'id': 'ecdata'})
    df = pd.read_html(str(table))[0]

    # Filter for only the data for Monday, 2/13
    df = df[df['Date'] == '02/13/2023']

    return(df)


def get_data2():
    url = 'https://www.earningswhispers.com/calendar'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Click "Show More" button to reveal all data
    show_more_button = soup.find('button', {'id': 'showMoreButton'})
    if show_more_button:
        show_more_url = 'https://www.earningswhispers.com' + show_more_button['onclick'].split("'")[1]
        show_more_response = requests.get(show_more_url)
        soup = BeautifulSoup(show_more_response.content, 'html.parser')

    # Find all the <li> tags with the specified class
    li_tags = soup.find_all("li", class_=['cor', 'cors'])

    # Extract the data from each tag using a list comprehension
    data = [
        {
            "Ticker": li_tag.find("div", class_="ticker").text.strip(),
            "Time": li_tag.find("div", class_="time").text.strip(),
            "EPS": li_tag.find("div", class_="estimate").text.strip(),
            "RevEst": li_tag.find("div", class_="revest").text.strip(),
            "RevGrowth": li_tag.find("div", class_="revgrowthprint").text.strip()
        }
        for li_tag in li_tags
    ]

    # Create a Pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(data)
    return(df)


if __name__ == '__main__':
    data_df = get_data2()
    print(data_df.info())
    print(data_df)