import requests
import pandas as pd
import time

# Setup & keyword definition

KORP_API_URL = "https://www.kielipankki.fi/korp/cgi-bin/korp/korp.cgi"

# Check exact names in Korp interface. Using placeholders for 2018-2023.
CORPORA = "S24_2018,S24_2019,S24_2020,S24_2021,S24_2022,S24_2023"

# Categorized dictionary
keywords_dict = {
    "healthy": ["vihannes", "vähärasvainen", "ruokavalio", "terveellinen", "hedelmä", "kuitu", "kasvis", "liha", "kotiruoka", "juures", "proteiini", "vitamiini", "salaatti", "pähkinä", "tuore"],
    "unhealthy": ["pikaruoka", "sokeri", "eines", "roskaruoka", "makeinen", "rasvainen", "epäterveellinen", "herkku", "pizza", "hampurilainen", "sipsi", "limsa", "alkoholi", "prosessoitu"],
    "sustainable": ["kasvissyöjä", "veg", "lähiruoka", "lihankulutus", "luomu", "ilmastoystävällinen", "hiilijalanjälki", "kotimainen", "hävikki"]
}

# Flat list of lemmas for API querying and text extraction
all_keywords = [kw for sublist in keywords_dict.values() for kw in sublist]


# Data extraction with Korp API


def fetch_concordances(keyword, start=0, end=999):
    # Fetches posts and threads even if a keyword is a part of a compound word
    cqp_query = f'[lemma=".*{keyword}.*"]' 
    
    params = {
        "command": "query",
        "corpus": CORPORA,
        "cqp": cqp_query,
        "start": start,
        "end": end,
        "defaultcontext": "1 paragraph", 
        "show": "word,lemma,pos",
        "show_struct": "text_datefrom,text_timefrom,text_thread_id" 
    }
    
    response = requests.get(KORP_API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching {keyword}: HTTP {response.status_code}")
        return None

def extract_keywords_from_text(text):
    text = str(text).lower()
    return [kw for kw in all_keywords if kw in text]

print("Starting Data Extraction...")
extracted_data = []

# Number of results to fetch per request
CHUNK_SIZE = 1000

for kw in all_keywords:
    print(f"Fetching data for: {kw}")
    
    # Initialize pagination variables for the current keyword
    start_idx = 0
    end_idx = CHUNK_SIZE - 1
    more_hits_remain = True
    
    while more_hits_remain:
        print(f"  -> Requesting hits {start_idx} to {end_idx}...")
        data = fetch_concordances(kw, start=start_idx, end=end_idx)
        
        # Check if we received valid data and that the kwic list isn't empty
        if data and 'kwic' in data and len(data['kwic']) > 0:
            for hit in data['kwic']:
                structs = hit.get('structs', {})
                date = structs.get('text_datefrom', '') 
                time_val = structs.get('text_timefrom', '')
                thread_id = structs.get('text_thread_id', 'Unknown_Thread') 
                
                tokens = hit.get('tokens', [])
                post_content = " ".join([token.get('word', '') for token in tokens])
                
                extracted_data.append({
                    'thread_id': thread_id,
                    'timestamp': f"{date} {time_val}".strip(),
                    'post_content': post_content
                })
            
            # If the API returned fewer items than our CHUNK_SIZE, 
            # it means we have reached the final page for this keyword.
            if len(data['kwic']) < CHUNK_SIZE:
                more_hits_remain = False
            else:
                # Otherwise, shift the window forward for the next request
                start_idx += CHUNK_SIZE
                end_idx += CHUNK_SIZE
                
        else:
            # If kwic is empty or missing, we are out of results
            more_hits_remain = False
            
        # Scheduling so we don't DDoS Kielipankki servers
        time.sleep(1)
        
# Convert to DataFrame
df = pd.DataFrame(extracted_data)

# Clean up duplicates
df = df.drop_duplicates(subset=['thread_id', 'post_content'])

# Re-extract keywords to catch all keywords in a post for co-occurrence
df['matched_keywords'] = df['post_content'].apply(extract_keywords_from_text)
df = df[df['matched_keywords'].map(len) > 0] # Filter out empty matches

print(f"\nExtraction complete. Total unique posts: {len(df)}")

# Save data to CSV

output_filename = "suomi24_food_data_2018_2023.csv"

# utf-8-sig is used so Excel handles Finnish characters correctly
df.to_csv(output_filename, index=False, encoding='utf-8-sig')

print(f"Successfully saved all data to {output_filename}")
