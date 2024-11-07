import streamlit as st
import os
import pickle
from db_client import get_query_embeddings, get_cloud_client
from openai import OpenAI
import time
import streamlit_analytics2
from google.cloud import firestore
import json
import pandas as pd
import re
from llama_index.vector_stores.postgres import PGVectorStore
from llama_index.core import VectorStoreIndex
from llama_index.core import Settings
from llama_index.core.memory import ChatMemoryBuffer
from llama_index.core.retrievers import VectorIndexRetriever

from llama_index.embeddings.openai import (
    OpenAIEmbedding,
    OpenAIEmbeddingMode,
    OpenAIEmbeddingModelType,
)

st.set_page_config(layout="wide", page_title="Fintrax Knowledge Center", page_icon="images/FINTRAX_EMBLEM_POS@2x_TRANSPARENT.png")

# Load environment variables

# Set up OpenAI client
OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']
client = OpenAI(api_key=OPENAI_API_KEY)
cloud_host = st.secrets["db_host"]
cloud_port = st.secrets["db_port"]
cloud_db_name = st.secrets["db_name"]
cloud_db_pwd = st.secrets["db_pwd"]
cloud_db_user = st.secrets["db_user"]

@st.cache_resource
def create_db_connection():
    cloud_aws_vector_store = PGVectorStore.from_params(
        database=cloud_db_name,
        host=cloud_host,
        password=cloud_db_pwd,
        port=cloud_port,
        user=cloud_db_user,
        table_name="800_chunk_400_overlap",
        embed_dim=756,  # openai embedding dimension
    )
    return cloud_aws_vector_store
cloud_aws_vector_store = create_db_connection()
@st.cache_resource
def vector_store_index(_cloud_aws_vector_store):
    
    index = VectorStoreIndex.from_vector_store(cloud_aws_vector_store,embed_model=OpenAIEmbedding(mode=OpenAIEmbeddingMode.SIMILARITY_MODE, model=OpenAIEmbeddingModelType.TEXT_EMBED_3_SMALL, dimensions=756))
    return index

index = vector_store_index(cloud_aws_vector_store)

# @st.cache_resource
# def create_chat_engine():
#     memory = ChatMemoryBuffer.from_defaults(token_limit=1000)

#     llm = OpenAI(model="gpt-4o", temperature=0.01)
#     chat_engine = index.as_chat_engine(llm=llm, system_prompt = '''
# GEBRUIK ALTIJD DE query_engine_tool OM TE ANTWOORDEN!!!
# BEANTWOORD ENKEL DE VRAAG ALS HET EEN FINANCIELE VRAAG IS!
# BEANTWOORD ENKEL ALS DE VRAAG RELEVANTE CONTEXT HEEFT!!
# Maak je antwoord overzichtelijk met opsommingstekens indien nodig.
# Jij bent een vertrouwd financieel expert in België die mensen helpt met perfect advies.
# GEEF VOLDOENDE INFORMATIE!
# ''',     similarity_top_k=10,
#     verbose=True)
#     return chat_engine
# chat_engine = create_chat_engine()

@st.cache_resource
def create_retriever():
    retriever = VectorIndexRetriever(
        index=index,
        similarity_top_k=10,
)
    return retriever

retriever = create_retriever()


#CSS injection that makes the user input right-aligned
st.markdown(
    """
<style>
    .st-emotion-cache-1c7y2kd {
        flex-direction: row-reverse;
        text-align: right;
    }
</style>
""",
    unsafe_allow_html=True,
)

email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'


def create_llm_prompt(question, retrieved_chunks): 
    # Combine the user's query and the retrieved document chunks into a single prompt
    prompt = f"Vraag van de gebruiker: {question}\n\n"
    prompt += "Relevante context:\n"
    
    for i, chunk in enumerate(retrieved_chunks):
        prompt += f"{i+1}. {chunk}\n"
    
    return prompt

col1, col2, col3 = st.sidebar.columns([1,6,1])
col2.image("images/thumbnail-modified.png")

firestore_string = st.secrets["FIRESTORE"]
firestore_cred = json.loads(firestore_string)
db = firestore.Client.from_service_account_info(firestore_cred)


def popup():
    if "user_id" not in st.session_state:
        st.toast("Vul aub een emailadres in.")


with st.sidebar.container():
    sidecol1, sidecol2, sidecode3 = st.columns(3)
    sidecol2.title("Acties")

    chatbot_button = st.button("Knowledge Center", use_container_width=True,on_click=popup)
    upload_button = st.button("Upload Files", use_container_width=True,on_click=popup)
    connecties = st.button("Connecties", use_container_width=True,on_click=popup)
    voorkeuren = st.button("Voorkeuren", use_container_width=True,on_click=popup)
    rapporten = st.button("Rapporten", use_container_width=True, on_click=popup)


# Maintain the user's selection between the buttons
if "user_id" not in st.session_state:
    st.title("Welkom bij het Knowledge Center!")
    with st.form("username_form"):
        
        username = st.text_input("Geef hier uw emailadres in om de applicatie te kunnen gebruiken", placeholder="Emailadres")
        
        # Form submission button
        submit_button = st.form_submit_button("Log in")

        # Check if the form is submitted
        if submit_button:
            if re.match(email_regex, username):
                if username:
                    # Set the username as user_id in session state
                    st.session_state['user_id'] = username
                    st.session_state["active_section"] = "Chatbot"
                    st.success(f"Succesvol ingelogd")

                    st.rerun()
                else:
                    st.error("Ongeldig emailadres. Vul aub een geldig emailadres in.")
                
                
            else:
                st.error("Vul aub een emailadres in.")

if st.secrets["PROD"] == "False" and "user_id" in st.session_state:
        if os.path.exists(f"analytics/{st.session_state.user_id}.json"):
            streamlit_analytics2.start_tracking(load_from_json=f"analytics/{st.session_state.user_id}.json")
        else:
            streamlit_analytics2.main.reset_counts()
            streamlit_analytics2.start_tracking()

else:
    streamlit_analytics2.start_tracking()
if "active_section" not in st.session_state:
    st.session_state["active_section"] = "Username"

if chatbot_button and "user_id" in st.session_state:
    st.session_state["active_section"] = "Chatbot"

if upload_button and "user_id" in st.session_state:
    st.session_state["active_section"] = "Upload Files"

if connecties and "user_id" in st.session_state:
    st.session_state["active_section"] = "Connecties"

if voorkeuren and "user_id" in st.session_state:
    st.session_state["active_section"] = "Voorkeuren"

if rapporten and "user_id" in st.session_state:
    st.session_state["active_section"] = "Rapporten"


# If Chatbot is selected
if st.session_state["active_section"] == "Chatbot":
    st.title("Knowledge Center")
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

    if "openai_model" not in st.session_state:
        st.session_state["openai_model"] = "gpt-4o"

    if "messages" not in st.session_state:
        st.session_state.messages = [{"role":"system", "content":"""Het is jouw taak om een feitelijk antwoord op de gesteld vraag op basis van de gegeven context en wat je weet zelf weet.
                                                                    BEANTWOORD ENKEL DE VRAAG ALS HET EEN FINANCIELE VRAAG IS!
                                                                    BEANTWOORD ENKEL ALS DE VRAAG RELEVANTE CONTEXT HEEFT!!
                                                                    Als de context codes of vakken bevatten, moet de focus op de codes en vakken liggen.
                                                                    Je antwoord MAG NIET iets zeggen als “volgens de passage” of “context”.
                                                                    Maak je antwoord overzichtelijk met opsommingstekens indien nodig.
                                                                    Jij bent een vertrouwd financieel expert in België die mensen helpt met perfect advies.
                                                                    GEEF VOLDOENDE INFORMATIE!""" },
                                        {"role": "assistant", "content": "Hallo, hoe kan ik je helpen? Stel mij al je financiële vragen!"}]

    for message in st.session_state.messages:
        if message["role"] != "system":
            if message["role"] == "assistant": 
                with st.chat_message(message["role"], avatar="images/thumbnail.png"):
                    st.markdown(message["content"])
            else:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

    if prompt := st.chat_input("Stel hier je vraag!"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant", avatar="images/thumbnail.png"):
            response = retriever.retrieve("Mijn echtgenoot is overleden dit jaar, welke codes moeten er nog ingevuld worden?")
            mess = ""
            for x in response:
                mess = mess + x.text
            st.session_state.messages.append({"role": "system", "content": mess})
            stream = client.chat.completions.create(
                model=st.session_state["openai_model"],
                messages=[
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages[-5:]
                ],
                stream=True,
            )
            
            response = st.write_stream(stream)
        st.session_state.messages.append({"role": "assistant", "content": response})

# If Upload Files is selected
elif st.session_state["active_section"] == "Upload Files":
    st.title("Upload Files")
    st.markdown("Deze functie is nog niet beschikbaar")
    uploaded_file = st.file_uploader("Choose a file", type=["txt", "pdf", "docx", "csv"])
    
    if uploaded_file is not None:
        # Display basic information about the uploaded file
        st.write(f"Uploaded file: {uploaded_file.name}")
        
        # Example: Reading text files and displaying the content
        if uploaded_file.type == "text/plain":
            content = uploaded_file.read().decode("utf-8")
            st.text_area("File Content", content, height=300)
        
        # Example for handling other file types (CSV, PDF, etc.) can be added here
        file_type = uploaded_file.type
        st.write(f"File type: {file_type}")


# If Upload Files is selected
elif st.session_state["active_section"] == "Connecties":
    st.title("Connecties")
    st.markdown("Deze functie is nog niet beschikbaar, verbinden met de API gaat nog geen data doorgeven.")

    fin1,fin2,fin3 = st.columns(3)
    with fin1:
        with st.container(border=True):
            fincol1, fincol2 = st.columns(2)
            fincol1.image("images/silverfin-logo.png")
            fincol2.subheader("Silverfin")
            fincol2.markdown("""Naam: Fiduciaire ABC
                                ID: 561
                                mark@abcaccouting.be""")
            verbindfin = st.button("Connecteer Silverfin", use_container_width=True)
            if verbindfin:
                with st.spinner("Verbinden..."):
                    time.sleep(3)
                st.success("Verbonden!")

    with fin2:
        with st.container(border=True):
            fincol1, fincol2 = st.columns(2)
            fincol1.image("images/mmf-logo.png")
            fincol2.subheader("MyMinFin")
            fincol2.markdown("""Naam: FIDUCIAIRE ABC 
                                support@fintrax.io""")
            verbindmy = st.button("Connecteer MyMinFin", use_container_width=True)
            if verbindmy:
                with st.spinner("Verbinden..."):
                    time.sleep(3)
                st.success("Verbonden!")


elif st.session_state["active_section"] == "Voorkeuren":
    st.title("Voorkeuren")
    st.markdown("Maak hier je standaardvragen of rapporten aan")
    # Initialize session state to store questions
    if 'questions' not in st.session_state:
        st.session_state.questions = []

    # Function to add a new question input field
    def add_question():
        st.session_state.questions.append("")
    
    def remove_question():
        if st.session_state.questions != 0:
            st.session_state.questions=st.session_state.questions[:-1]


    add, rem, _ = st.columns([1,1,30])
    # Button to add a new question dynamically
    add.button('\+', on_click=add_question)
    rem.button('\-', on_click=remove_question)

    # Display the input fields for the questions dynamically
    for i, question in enumerate(st.session_state.questions):
        st.session_state.questions[i] = st.text_input(f"Question {i+1}:", value=question, key=f"question_{i}")

    # Button to save the form
    if st.button("Opslaan"):
        st.success("Opgeslagen!")



    


elif st.session_state["active_section"] == "Rapporten":
    st.title("Rapporten")
    st.markdown("Deze functie is nog niet beschikbaar")

    st.dataframe({"Vragen": st.session_state.questions}, width=800)
    if st.button('Voer Uit'):
        time.sleep(3)
        st.success("Uitgevoerd!")

if st.secrets["PROD"] == "False" and "user_id" in st.session_state:
    streamlit_analytics2.stop_tracking(save_to_json=f"analytics/{st.session_state.user_id}.json", unsafe_password=st.secrets["ANALYTICS_PWD"])
    doc_ref = db.collection('users').document(str(st.session_state.user_id))
    analytics_data = pd.read_json(f"analytics/{st.session_state.user_id}.json")
    doc_ref.set(analytics_data.to_dict(), merge=True)
else:
    streamlit_analytics2.stop_tracking()
