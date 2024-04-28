import pandas as pd # type: ignore
import re
from fuzzywuzzy import fuzz # type: ignore
from unidecode import unidecode # type: ignore
from collections import Counter


brands_list_1 = 'brands_list_1_short.csv'
brands_list_2 = 'brands_list_2_short.csv'

brands1 = pd.read_csv(brands_list_1, header=None, names=['brand'], on_bad_lines='skip')
brands2 = pd.read_csv(brands_list_2, header=None, names=['brand'], on_bad_lines='skip')

common_words = set(['international', 'co', 'beton', 'ltd', 'group','products','france','sa']) 

def tokenize(s):
    return Counter(re.findall(r'\b\w+\b', s.lower()))

def preprocess_brand(brand):
    brand = unidecode(brand)
    tokens = tokenize(brand)
    tokens = {word: count for word, count in tokens.items() if word not in common_words}
    return ' '.join(sorted(tokens))

brands1['brand'] = brands1['brand'].astype(str).apply(preprocess_brand)
brands2['brand'] = brands2['brand'].astype(str).apply(preprocess_brand)

def find_matches(brand, choices, threshold=70):
    matches = [(choice, fuzz.token_set_ratio(brand, choice)) for choice in choices] # type: ignore
    return [match for match in matches if match[1] >= threshold]

matches = []
for brand in brands1['brand']:
    matched_brands = find_matches(brand, brands2['brand'].tolist())
    for match in matched_brands:
        matches.append((brand, match[0], match[1]))

matches_df = pd.DataFrame(matches, columns=['Brand List 1', 'Matched Brand List 2', 'Match Score'])
matches_df.to_csv('matched_brands.csv', index=False)
print(matches_df.head(10))
