from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from multiprocessing import Pool

CURRENT_DATETIME = datetime.now()


def get_all_french_titles() -> list[int]:
    """
    Obtain book IDs of all French language books on free E-book platform Project Gutenburg
    """
    url = "https://www.gutenberg.org/browse/languages/fr"

    response = requests.get(url)
    html_content = response.content
    soup = BeautifulSoup(html_content, "html.parser")

    title_items = soup.find_all("ul")

    french_book_ids = []

    for item in title_items:
        if 'French' in item.text and 'English' not in item.text:
            title_link = item.find("a")
            if title_link:
                id_str = title_link.get("href").strip('/ebooks/')
                try:
                    french_book_ids.append(int(id_str))
                except ValueError:
                    continue

    return french_book_ids


def calculate_word_count(sentence):
    return len(sentence.split())


def get_sentences(book_id: int) -> pd.DataFrame:
    """
    Obtaining books text from Gutenberg and cleaning data to match requirements
    """

    url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    response = requests.get(url)

    # Remove unwanted ASCII characters
    text_content = (response.text.replace("\r\n", " ").replace("\u202f", " ")
                    .replace("\xa0", " ").replace("—\u2009", " ").strip())

    # Split book into individual sentences and remove trailing whitespaces, intro & outro
    sentences = re.split(r'[.!?]', text_content)
    sentences = [sentence.strip(' -"') for sentence in sentences[50:-700] if sentence.strip()]

    # Set times series so every entry is matched to a time that is a minute later than the last entry
    times = [CURRENT_DATETIME + timedelta(minutes=i) for i in range(len(sentences))]

    # Place sentences within pandas df for easy manipulation
    df = pd.DataFrame({"comment": sentences, "created_at": times})
    df['word_count'] = df['comment'].apply(calculate_word_count)

    # Filter the DataFrame to include only the sentences with more than 5 words
    filtered_df = df[df['word_count'] > 5]
    filtered_df = filtered_df.drop(columns='word_count')

    # Filter the DataFrame to include only the sentences with more than 5 words
    filtered_df = filtered_df.drop_duplicates()

    return filtered_df


if __name__ == "__main__":

    french_book_ids = get_all_french_titles()

    # Play around with number of 'french_book_ids' being processed to alter quantity of sentences being output
    with Pool(5) as p:
        result = p.map(get_sentences, french_book_ids)

    result_df = pd.concat(result[2500000:], ignore_index=True)
    result_df.to_csv("french_sentences.csv", index=True)
