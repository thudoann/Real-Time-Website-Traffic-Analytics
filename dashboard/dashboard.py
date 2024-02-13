import streamlit as st
import pandas as pd
import dataikuapi
import pandas as pd
import numpy as np
import pydeck as pdk
from dateutil import parser
from consumer import consume
from producer import produce
import requests
import json
import time

def preprocess(raw_logs_df):
    raw_logs_df = raw_logs_df.drop(['visitor_params', 'session_params', 'event_params',
                                    'type', 'client_addr','location','session_id', 'visitor_id'], axis=1)
    # # assumed the rest of the customer boolean values are zeros 
    # for simple and quick data balancing, bootstrap the data to balance the data where customer = 1, with a factor of 3 
    df_customer = raw_logs_df[raw_logs_df['customer'] == 1]
    raw_logs_df['server_ts'] = pd.to_datetime(raw_logs_df['server_ts'])
    #split server_ts into year, month, day, and hour
    raw_logs_df['server_ts'] = raw_logs_df['server_ts'].dt.strftime('%Y-%m-%d-%H')

    #if df["'br_lang'"] is Nan, replace with 'other'
    # for languages that start with xx-, group all of them into xx
    raw_logs_df['br_lang'] = raw_logs_df['br_lang'].apply(lambda x: x.split('-')[0])
    common_langs = raw_logs_df['br_lang'].value_counts()
    common_langs = common_langs[common_langs > 10]
    # for languages that have counts less than 10, group them in an 'other' category
    raw_logs_df['br_lang'] = raw_logs_df['br_lang'].apply(lambda x: x if x in common_langs else 'other')
    # for languages that start with xx-, group all of them into xx
    #if df["'referer'"] is Nan, replace with 'other'
    raw_logs_df['referer'] = raw_logs_df['referer'].fillna('other')
    raw_logs_df['referer'] = raw_logs_df['referer'].apply(lambda x: x.split('-')[0])
    common_referers = raw_logs_df['referer'].value_counts()
    common_referers = common_referers[common_referers > 10]
    # for languages that have counts less than 10, group them in an 'other' category
    raw_logs_df['referer'] = raw_logs_df['referer'].apply(lambda x: x if x in common_referers else 'other')
    # recategorize user_guide to lower categories
    temp = raw_logs_df.user_agent.values.tolist()
    final_cats = []
    keywords = ['windows', 'macintosh', 'linux', 'other']
    for i in temp:
        if any(x in i.lower() for x in keywords):
            # get the first keyword that matches
            for x in keywords:
                if x in i.lower():
                    final_cats.append(x)
                    break
        else:
            
            final_cats.append('other')
    raw_logs_df['user_agent'] = final_cats
    return raw_logs_df

def parse_date(date_string):
    try:
        return parser.parse(date_string)
    except ValueError:
        # Handle parsing error, log, or provide a default date
        return pd.Timestamp.now()
    
def run_website():
    # Replace the API URL and key with your actual Dataiku API information
    api_url = "https://api-c90e8bc2-c6a8da23-dku.eu-west-3.app.dataiku.io/"
    project_key = "v4"

    # Initialize Dataiku API client
    client = dataikuapi.APINodeClient(api_url, project_key)

    # Set the page configuration
    st.set_page_config(
        page_title="Visitors Dashboard",
        page_icon="🤖",
        layout="wide",  # Set the layout to wide for increased width
        initial_sidebar_state="expanded"
    )

    # Add a title to the app
    st.title("Customer Prediction App")

    # Add a subtitle to the app
    st.subheader("Real-time one visitor session data")

        #make streamlit button to predict one record
    if st.button("Predict one record"):
        # Display a box that has a json representation which 
        produce(1, 'cr_pred')
        one_consumption = consume(1, 'cr_pred')
        print(one_consumption)
        st.json(one_consumption)
        # get json as python dictionary
        # get the value of the key 'visitor_id'
        dict_ = one_consumption
        # create a one row pandas dataframe from the dictionary
        one_value_df = pd.DataFrame([dict_])
        values = preprocess(one_value_df)[['server_ts', 'referer', 'user_agent', 'br_width', 'br_height','sc_width', 'sc_height', 'br_lang', 'tz_off']]
        prediction = client.predict_record("customer_classifier_v4", dict(zip(values.columns, values.values[0])))['result']['prediction']
        # Display prediction result
        st.subheader("Prediction Result:")
        if prediction == '1.0':
            # create a green box with the text "A new customer is on the way!"
            st.success("Prediction is 1. A new customer is on the way!")
        else:
            # create a red box with the text "No new customer for today"
            st.error("Prediction is 0. No new customer for today :( (but maybe tomorrow :D )")


    st.subheader("Predict your input data via the sidebar")
    # project = client.get_project(project_key)

    # Load the sample data
    data = {
        "server_ts": ["2014-03-01-03", "2014-03-01-03", "2014-03-01-06", "2014-03-01-06", "2014-03-01-06",
                    "2014-03-01-03", "2014-03-01-03", "2014-03-01-06", "2014-03-01-06", "2014-03-01-06"
                    "2014-03-01-03", "2014-03-01-03", "2014-03-01-06", "2014-03-01-06", "2014-03-01-06"],
        "referer": ['other', 'dataiku', 'strata', 'kaggle', 'google',
                    'datatau','journaldunet', 'agoranov', 'practicalquant', 'linkedin',
                    'bananadata', 'kdnuggets', 'datajob2013', 'bing'],
        "user_agent": ['macintosh', 'linux', 'windows', 'other',
                    'macintosh', 'linux', 'windows', 'other',
                    'macintosh', 'linux', 'windows', 'other',
                    'macintosh', 'linux'],
        "br_width": [1920, 1680, 1183, 1366, 1920,
                    1920, 1680, 1183, 1366, 1920,
                    1920, 1680, 1183, 1366],
        "br_height": [986, 963, 658, 664, 995,
                    986, 963, 658, 664, 995,
                    986, 963, 658, 664],
        "sc_width": [1920, 1680, 1280, 1366, 1920,
                    1920, 1680, 1280, 1366, 1920,
                    1920, 1680, 1280, 1366],
        "sc_height": [1080, 1050, 800, 768, 1080,
                    1080, 1050, 800, 768, 1080,
                    1080, 1050, 800, 768],
        "br_lang": ['en', 'fr', 'ja', 'pt', 'other', 'zh', 'es', 'ru', 'da', 'de',
        'ca', 'nl', 'tr', 'pl'],
        "tz_off": [300, -660, 300, -480, 420,
                300, -660, 300, -480, 420,
                300, -660, 300, -480],
    }
    df = pd.DataFrame(data)
    # Sidebar with input widgets
    st.sidebar.subheader("Input Features")

    # Dropdowns for categorical columns
    for col in ["referer", "user_agent", "br_lang"]:
        df[col] = st.sidebar.selectbox(col, df[col].unique())

    # Text inputs for numerical columns
    for col in ["br_width", "br_height", "sc_width", "sc_height", "tz_off"]:
        df[col] = st.sidebar.number_input(col, value=df[col].mean())

    # Date input for the timestamp
    # df["server_ts"] = st.sidebar.date_input("server_ts", pd.to_datetime(df["server_ts"]).iloc[0])
    date_string = st.sidebar.text_input("server_ts", df["server_ts"].iloc[0])
    df["server_ts"] = parse_date(date_string)
    # Button to trigger prediction
    if st.sidebar.button("Predict"):
        # Prepare record for prediction
        record_to_predict = df.iloc[0].to_dict()

        # Convert Timestamp to string
        record_to_predict["server_ts"] = str(record_to_predict["server_ts"])

        # Add a placeholder for the predicted result
        record_to_predict["customer"] = 0

        # Make prediction using Dataiku API
        prediction = client.predict_record("customer_classifier_v4", record_to_predict)
        
        # Display prediction result
        st.subheader("Prediction Result:")

        prediction = prediction["result"]['prediction']

        if prediction == '1.0':
            # create a green box with the text "A new customer is on the way!"
            st.success("Prediction is 1. A new customer is on the way!")
        else:
            # create a red box with the text "No new customer for today"
            st.error("Prediction is 0. No new customer for today :( (but maybe tomorrow :D )")

    # add a vertical space
    st.markdown("---")
    st.subheader("Predict your input data via the sidebar")
    # Add a button to start the consumer
    if st.button("Display Dashboard", key="start_producer"):
        produce(20)  # Call the produce function 100 item
        consume(20)  # Call the consume function 100 item
        st.markdown("---")
        def load_data(f):
            data = pd.read_csv(f)
            # Optional: Parse server_ts as datetime
            data['server_ts'] = pd.to_datetime(data['server_ts'])
            return data

        df = load_data('./newlogs.csv') 

        st.subheader('Recent real-time website visits dashboard' )

        dfc = pd.read_csv('./country.csv') 
        # Display a map
        st.header('Number of visitors over the globe')
        # Removing the 'Unnamed: 0' column as it seems to be an index


        # PyDeck chart
        st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=pdk.ViewState(
            latitude=46.76,
            longitude=2.4,
            zoom=7,
            pitch=50,
        ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=dfc,
                get_position='[longitude, latitude]',
                get_color='[200, 30, 0, 160]',  # Customize as needed
                get_radius='city * 5000',  # Example: scale radius by customer count
                #pickable=True
                ),
            ],
        ))


        # App title
        st.subheader('Enhanced Interactive Dashboard for Web Logs')

        # Layout using columns
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Visits", len(df))
            unique_visitors = df['visitor_id'].nunique()
            st.metric("Unique Visitors", unique_visitors)

        with col2:
            unique_sessions = df['session_id'].nunique()
            st.metric("Unique Sessions", unique_sessions)
            most_visited_pages = df['location'].value_counts().head(5)
            st.bar_chart(most_visited_pages)

        with col3:
            # Visitor Browser Types
            st.metric('Top 5 countries by number of visitors',5)
            browser_usage = df['country'].value_counts().head(5)
            st.bar_chart(browser_usage)

        # Top 5 Referring Sites
        st.subheader('Top 5 Referring Sites')
        top_referrers = df['referer'].value_counts().head(5)
        st.bar_chart(top_referrers)

        # Most Active Times
        st.subheader('Most Active Times')
        df['hour'] = df['server_ts'].dt.hour
        visits_by_hour = df['hour'].value_counts().sort_index()
        st.bar_chart(visits_by_hour)

        # Custom CSS to style the app
        st.markdown("""
        <style>
        .stMetricLabel, .stDataFrame, .stExpander > div:first-child {
            font-size: 16px;
        }
        .stMetricValue {
            font-size: 20px !important;
        }
        </style>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    run_website()