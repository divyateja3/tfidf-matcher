import numpy as np
import pandas as pd
import sqlalchemy

from fuzzywuzzy import fuzz
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ftfy import fix_text

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors


class Connection:

    def __init__(self):
        CONNECTION_STRING = 'postgresql://hfdb:123AdminRiskAdv!@radient-prod.csx3vkjw93hn.us-east-1.rds.amazonaws.com:5433/radient_prod'
        engine = sqlalchemy.create_engine(CONNECTION_STRING)

        try:
            self.connection = engine.connect()
            print("Connected to server : SUCCESS")
        except SQLAlchemyError as error:
            raise f'[CONNECTION ERROR] {error}'

    def sql_to_frame(self, filename):

        with open(filename, 'r') as file:
            query = file.read()

        dataframe = pd.read_sql(text(query), self.connection)

        return dataframe
    
    def frame_to_sql(self, dataframe, table_name):
        
        dataframe.to_sql(table_name, self.connection, if_exists='append', index=False)
        
        return True

    def close(self):
        self.connection.close()


def get_fuzzy_matches_df(matches_df, dataframe_a, dataframe_b):
    def _match_scores(data_a, data_b):

        left_values = list(set(data_a.related_partners))
        right_values = data_b.direct_owners.to_list()

        match_df = []

        for partner in left_values:
            best_owner, best_raio = None, 0
            for owner in right_values:
                ratio = fuzz.SequenceMatcher(None, partner, owner).ratio()
                if ratio > best_raio:
                    best_owner = owner
                    best_raio = ratio
            match_df.append((partner, best_owner, best_raio))

        return pd.DataFrame(match_df, columns=['related_partner', 'direct_owner', 'owners_ratio'])

    matches_list = []

    for idx, entry in matches_df.iterrows():
        cik_no_fund, crd_no_fund = entry.cik_no_fund, entry.crd_no_fund

        table_a = dataframe_a[dataframe_a.cik_no_related_partners == cik_no_fund]
        table_b = dataframe_b[dataframe_b.crd_no_owners == crd_no_fund]

        matches = _match_scores(table_a, table_b)
        matches.columns = ['related_partners', 'direct_owners_fund', 'owners_fund_ratio']

        matches['cik_no_fund'] = cik_no_fund
        matches['crd_no_fund'] = crd_no_fund

        matches_list.append(matches)

    matches = pd.DataFrame()

    for data in matches_list:
        if not data.empty:
            if not matches.empty:
                matches = pd.concat([matches, data], join='inner', ignore_index=True)
            else:
                matches = data

    return matches


def get_matches_df(dataframe_a, dataframe_b, **props):

    def _ngrams(string, n=2):
        string = str(string)
        string = fix_text(string)  # fix text
        n_grams = zip(*[string[i:] for i in range(n)])
        return [''.join(ngram) for ngram in n_grams]

    column_a, column_b = props.get('column_a'), props.get('column_b')
    map_from, map_to = props.get('map_from'), props.get('map_to')

    right_values = dataframe_b[column_b].unique()

    vectorizer = TfidfVectorizer(min_df=1, analyzer=_ngrams, lowercase=False)
    tfidf = vectorizer.fit_transform(right_values)
    nbrs = NearestNeighbors(n_neighbors=1, n_jobs=-1).fit(tfidf)

    # column to match against in the data
    left_values = set(dataframe_a[column_a])  # set used for increased performance

    # matching query
    def _getNearestN(query):
        queryTFIDF_ = vectorizer.transform(query)
        distances, indices = nbrs.kneighbors(queryTFIDF_)
        return distances, indices

    distances, indices = _getNearestN(left_values)

    left_values = list(left_values)  # need to convert back to a list

    matches = []
    for i, j in enumerate(indices):
        temp = [left_values[i], right_values[j][0], distances[i][0]]
        matches.append(temp)

    matches = pd.DataFrame(matches, columns=[f'{map_from}', f'matched_{map_to}', f'{map_to}_confidence'])

    return matches


def get_merged_matches(matches_dataframe, dataframe_a, dataframe_b, **props):

    column_a, column_b = props.get('column_a'), props.get('column_b')
    map_a, map_b = props.get('map_a'), props.get('map_b')

    for dataframe, column, _map in [(dataframe_a, column_a, map_a), (dataframe_b, column_b, map_b)]:
        for col in dataframe.columns:
            col_dict = pd.Series(dataframe[col].values, index=dataframe[_map]).to_dict()
            col_matches = [col_dict[value] for value in matches_dataframe[column]]
            matches_dataframe = pd.concat([matches_dataframe, pd.Series(col_matches, name=f'{col}')], axis=1)

    return matches_dataframe


def _preprocess_formd_funds(dataFrame):
    def _transform_formd_funds(value):
        value = value.upper()
        remove_terms = {
            'LLC', 'LP', 'LTD', 'INC', 'CO', 'CORP'
        }
        newVal = []
        for name in value.split():
            if name in remove_terms:
                continue
            alphaName = filter(lambda x: not x.isdigit(), name)
            alphaName = ''.join(alphaName).strip()
            newVal.append(alphaName)
        return ' '.join(newVal)

    # Transform form_d_funds
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.replace('(', '', regex=False)
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.replace(')', '', regex=False)
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.replace(',', '', regex=False)
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.replace('.', '', regex=False)
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.replace('@', '', regex=False)
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.replace('/', ' ', regex=False)
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.replace('-', ' ', regex=False)
    dataFrame.form_d_funds = dataFrame.form_d_funds.apply(_transform_formd_funds)
    dataFrame.form_d_funds = dataFrame.form_d_funds.str.title()

    return dataFrame


def _preprocess_formadv_funds(dataFrame):

    def _transform_formadv_funds(value):
        value = value.upper()
        remove_terms = {
            'LLC', 'LP', 'LTD', 'INC', 'CO', 'CORP', 'LLLP', 'LLLC'
        }
        newVal = []
        for name in value.split():
            if name in remove_terms:
                continue
            alphaName = filter(lambda x: not x.isdigit(), name)
            alphaName = ''.join(alphaName).strip()
            newVal.append(alphaName)
        return ' '.join(newVal)

    # Transform form_adv_funds
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.replace('(', '', regex=False)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.replace(')', '', regex=False)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.replace(',', '', regex=False)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.replace('.', '', regex=False)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.replace('@', '', regex=False)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.replace('/', ' ', regex=False)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.replace('-', ' ', regex=False)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.apply(_transform_formadv_funds)
    dataFrame.form_adv_funds = dataFrame.form_adv_funds.str.title()

    return dataFrame


def _preprocess_related_partners(dataFrame):
    def _transform_related_partners(value):
        value = value.upper()
        remove_terms = {
            'LLC', 'LTD', 'INC', 'NA', 'NONE'
        }
        newVal, dupCheck = list(), set()
        for name in value.split():
            if name in remove_terms:
                continue
            alphaName = filter(lambda x: not x.isdigit(), name)
            alphaName = ''.join(alphaName).strip()
            if alphaName not in dupCheck:
                newVal.append(alphaName)
                dupCheck.add(alphaName)

        return ' '.join(newVal)

    # Transform related_partners
    dataFrame.related_partners = dataFrame.related_partners.str.replace('(', '', regex=False)
    dataFrame.related_partners = dataFrame.related_partners.str.replace(')', '', regex=False)
    dataFrame.related_partners = dataFrame.related_partners.str.replace(',', '', regex=False)
    dataFrame.related_partners = dataFrame.related_partners.str.replace('.', '', regex=False)
    dataFrame.related_partners = dataFrame.related_partners.str.replace('@', '', regex=False)
    dataFrame.related_partners = dataFrame.related_partners.str.replace('/', ' ', regex=False)
    dataFrame.related_partners = dataFrame.related_partners.str.replace('-', ' ', regex=False)
    dataFrame.related_partners = dataFrame.related_partners.apply(_transform_related_partners)
    dataFrame.related_partners = dataFrame.related_partners.str.title()

    return dataFrame


def _preprocess_direct_owners(dataFrame):
    def _transform_direct_owners(value):
        value = value.upper()
        remove_terms = {
            'LLC', 'LTD', 'INC', 'NA', 'NONE'
        }
        newVal, dupCheck = list(), set()
        for name in value.split():
            if name in remove_terms:
                continue
            alphaName = filter(lambda x: not x.isdigit(), name)
            alphaName = ''.join(alphaName).strip()
            if alphaName not in dupCheck:
                newVal.append(alphaName)
                dupCheck.add(alphaName)

        return ' '.join(newVal)

    # Transform direct_owners
    dataFrame.direct_owners = dataFrame.direct_owners.str.replace('(', '', regex=False)
    dataFrame.direct_owners = dataFrame.direct_owners.str.replace(')', '', regex=False)
    dataFrame.direct_owners = dataFrame.direct_owners.str.replace(',', '', regex=False)
    dataFrame.direct_owners = dataFrame.direct_owners.str.replace('.', '', regex=False)
    dataFrame.direct_owners = dataFrame.direct_owners.str.replace('@', '', regex=False)
    dataFrame.direct_owners = dataFrame.direct_owners.str.replace('/', ' ', regex=False)
    dataFrame.direct_owners = dataFrame.direct_owners.str.replace('-', ' ', regex=False)
    dataFrame.direct_owners = dataFrame.direct_owners.apply(_transform_direct_owners)
    dataFrame.direct_owners = dataFrame.direct_owners.str.title()

    return dataFrame


def preprocess(dataframe: pd.DataFrame, **parm):

    if parm.get('formd_funds'):
        return _preprocess_formd_funds(dataframe)

    if parm.get('formadv_funds'):
        return _preprocess_formadv_funds(dataframe)

    if parm.get('related_partners'):
        return _preprocess_related_partners(dataframe)

    if parm.get('direct_owners'):
        return _preprocess_direct_owners(dataframe)

    return pd.DataFrame(dataframe)


radient_prod = Connection()

print("Fetching Data")

# Fetch data from SQL queries
formd_funds = radient_prod.sql_to_frame('models/formd_funds.sql')
formadv_funds = radient_prod.sql_to_frame('models/formadv_funds.sql')
related_partners = radient_prod.sql_to_frame('models/related_partners.sql')
direct_owners = radient_prod.sql_to_frame('models/direct_owners.sql')

formd_funds = preprocess(formd_funds, formd_funds=True)
formadv_funds = preprocess(formadv_funds, formadv_funds=True)

print("Generating Mapping")

# Get mapping results
matches_funds = get_matches_df(formd_funds, formadv_funds, column_a='form_d_funds', column_b='form_adv_funds', map_from='fund', map_to='fund')
matches_funds = get_merged_matches(matches_funds, formd_funds, formadv_funds, column_a='fund', column_b='matched_fund', map_a='form_d_funds', map_b='form_adv_funds')

# Formatting Data Frame
filter_columns = [
    'form_d_firm_id', 'cik_no_fund', 'fund',
    'form_adv_firm_id', 'firm_id', 'crd_no_fund', 'matched_fund', 'fund_confidence',
]
matches_funds = matches_funds[filter_columns]

related_partners = preprocess(related_partners, related_partners=True)
direct_owners = preprocess(direct_owners, direct_owners=True)

matches_owners = get_fuzzy_matches_df(matches_funds, related_partners, direct_owners)
match_funds_owners = pd.merge(matches_funds, matches_owners, how='inner')

filter_columns = [
    'form_d_firm_id', 'cik_no_fund', 'fund',
    'form_adv_firm_id', 'firm_id', 'crd_no_fund', 'matched_fund', 'fund_confidence',
    'related_partners',
    'direct_owners_fund', 'owners_fund_ratio'
]
match_funds_owners = match_funds_owners[filter_columns]

# Sort by fund_confidence
match_funds_owners = match_funds_owners.sort_values(by='fund_confidence', ignore_index=True)

# Round off confidence & ratio
match_funds_owners = match_funds_owners.round({'fund_confidence': 3, 'firm_confidence': 3, 'owners_firm_ratio': 3, 'owners_fund_ratio': 3})

# Threshold
fund_threshold, owners_threshold = 0.512, 0.555
match_funds_owners = match_funds_owners[(match_funds_owners.fund_confidence < fund_threshold) | (match_funds_owners.owners_fund_ratio > owners_threshold)]

# Drop Null matches
match_funds_owners.dropna(subset=['fund', 'matched_fund'], inplace=True)
match_funds_owners = match_funds_owners[match_funds_owners.fund_confidence != 1]

# Drop duplicates
match_funds_owners = match_funds_owners[match_funds_owners['owners_fund_ratio'] == match_funds_owners.groupby('cik_no_fund')['owners_fund_ratio'].transform(max)]

# Inverse fund_confidence
match_funds_owners['fund_confidence'] = 1 - match_funds_owners['fund_confidence']


# Add cik_no_fund_nan Column
match_funds_owners['cik_no_fund_nan'] = np.nan

# Rename columns
filter_columns = [
    'fund', 'cik_no_fund',
    'matched_fund', 'crd_no_fund', 'cik_no_fund_nan', 'fund_confidence',
    'firm_id', 'form_d_firm_id', 'form_adv_firm_id',
]
match_funds_owners = match_funds_owners[filter_columns]
match_funds_owners.rename({
    'fund': 'entity_name', 'cik_no_fund': 'formd_cik',
    'matched_fund': 'firm_name', 'crd_no_fund': 'firm_crd', 'cik_no_fund_nan': 'firm_cik', 'fund_confidence': 'match_score',
    'form_d_firm_id': 'firm_formd_value_id'
}, inplace=True, axis=1)

print("Updating Results to Table")

radient_prod.frame_to_sql(match_funds_owners, 'formd_formdfirmmapping')

radient_prod.close()
