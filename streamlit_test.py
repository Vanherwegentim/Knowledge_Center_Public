import json
import os
import time
from uuid import uuid4
import re
import pandas as pd
import streamlit as st
import streamlit_analytics2
import streamlit_authenticator as stauth
from google.cloud import firestore
from llama_index.agent.openai import OpenAIAgent
from llama_index.core import VectorStoreIndex
from llama_index.core.tools import FunctionTool, QueryEngineTool
from llama_index.core.memory.chat_memory_buffer import ChatMemoryBuffer
from bot_queries.queries import voorafbetaling
import pandas as pd
from llama_index.embeddings.openai import (
    OpenAIEmbedding,
    OpenAIEmbeddingMode,
    OpenAIEmbeddingModelType,
)
from llama_index.llms.openai import OpenAI
from llama_index.vector_stores.postgres import PGVectorStore

from calculator.calculator import bereken, vergelijk_op_basis_van
from tools import (
    account_details,
    add,
    companies_ids_api_call,
    company_api_call,
    has_tax_decreased_api_call,
    multiply,
    period_api_call,
    period_id_fetcher,
    reconciliation_api_call,
    get_date,
)

st.set_page_config(
    layout="wide",
    page_title="Fintrax Knowledge Center",
    page_icon="images/FINTRAX_EMBLEM_POS@2x_TRANSPARENT.png",
)


# Set up OpenAI client
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
COLLECTION_NAME = "openai_vectors"  # Milvus collection name
client = OpenAI(api_key=OPENAI_API_KEY)

# Authentication
import yaml
from yaml.loader import SafeLoader

with open("credentials.yaml") as file:
    config = yaml.load(file, Loader=SafeLoader)
authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)
if st.session_state.get("authentication_status") and "active_section" not in st.session_state:
    st.session_state["active_section"] = "Chatbot"

# Vector DB Setup
cloud_host = st.secrets["db_host"]
cloud_port = st.secrets["db_port"]
cloud_db_name = st.secrets["db_name"]
cloud_db_pwd = st.secrets["db_pwd"]
cloud_db_user = st.secrets["db_user"]
cloud_db_table_name = st.secrets["db_table_name"]


@st.cache_resource
def create_db_connection():
    cloud_aws_vector_store = PGVectorStore.from_params(
        database=cloud_db_name,
        host=cloud_host,
        password=cloud_db_pwd,
        port=cloud_port,
        user=cloud_db_user,
        table_name=cloud_db_table_name,
        embed_dim=756,  # openai embedding dimension
    )
    return cloud_aws_vector_store


cloud_aws_vector_store = create_db_connection()
email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'


@st.cache_resource
def vector_store_index(_cloud_aws_vector_store):
    index = VectorStoreIndex.from_vector_store(
        cloud_aws_vector_store,
        embed_model=OpenAIEmbedding(
            mode=OpenAIEmbeddingMode.SIMILARITY_MODE,
            model=OpenAIEmbeddingModelType.TEXT_EMBED_3_SMALL,
            dimensions=756,
        ),
    )
    return index


index = vector_store_index(cloud_aws_vector_store)

system_prompt = """
Je bent een vertrouwde financiële expert in België die het personeel van het bedrijf VGD helpt met perfect advies. Het is jouw taak om een feitelijk en volledig antwoord te geven op de gestelde vraag op basis van de informatie die je verkrijgt via de beschikbare tools.
**Belangrijke richtlijnen:**

- **Gebruik altijd de tool 'Financiele_informatie' om informatie op te halen voor elke vraag.** Baseer je antwoorden uitsluitend op informatie uit deze tool.

- ALS DE VRAAG BETREKKING HEEFT OP SPECIFIEKE CODES, VAKKEN OF FINANCIELE TOOLS, LEG DAN DE FOCUS OP HET UITLEGGEN VAN DIE CODES!!!

- Geef voldoende informatie, maak je antwoord dus wat langer.

- **Vermijd het gebruik van zinnen zoals "volgens de passage" of "volgens de context" in je antwoord.**

- Maak je antwoord overzichtelijk, gebruik opsommingstekens indien nodig, en zorg voor een duidelijke structuur.

- **Geef voldoende en nauwkeurige informatie** om de vraag volledig te beantwoorden.

- Schrijf in helder en professioneel Nederlands, met de juiste terminologie.

- Het is goed om te weten dat dossiers en bedrijven hetzelfde worden gezien in de database functies. Dus als iemand vraagt achter een dossier met een naam dan gaat het eigenlijk over een bedrijf. Een dossier is een company in de database

- Voer een calculatie altijd uit als het gevraagd is. 

- Zeg nooit, "Ik ga dit uitrekenen" zonder het ook echt te doen"""


llm = OpenAI(model="gpt-4o", temperature=0, system_prompt=system_prompt)

description = """
'Financiele_informatie' is een uitgebreide RAG-database die diepgaande informatie bevat over:

- **Belgische belastingen en financiële wetgevingen**
- **Fiscale codes en relevante vakken**
- **Software en programma's die in de financiële sector worden gebruikt (zoals Excel)**
- **Best practices en tools voor financiële professionals**

Gebruik deze tool altijd om:

- **Algemene en specifieke financiële vragen** te beantwoorden.
- **Informatie over belastingcodes, vakken en financiële software** te verstrekken.
- **Actuele wetgevingen, regelgeving en technologische ontwikkelingen** te raadplegen binnen de financiële sector.

**Belangrijk:** Baseer al je antwoorden uitsluitend op de informatie die je via deze tool verkrijgt. Voeg geen externe informatie toe, zelfs niet als je deze kent.
"""


query_engine = index.as_query_engine(llm=llm)
budget_tool = QueryEngineTool.from_defaults(
    query_engine,
    name="Financiele_informatie",
    description=description,
)

from utils import (
    get_db_connection,
)
def list_tables():
    """
    Geeft een lijst van alle tabellen in het schema.
    Returns:
        list: Lijst van tabelnamen in het 'public' schema.
    """
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    return result


def describe_tables(table_name: str):
    """
    Geeft de kolomnamen en datatypes van de opgegeven tabel.
    Args:
        table_name (str): Naam van de tabel om te beschrijven.
    Returns:
        list: Lijst van kolomnamen en hun datatypes voor de opgegeven tabel.
    """
    sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    return result


if "data" not in st.session_state:
    st.session_state.data = None

def load_data(sql_query: str):
    """
    Voert een SQL-query uit en retourneert het resultaat. Het resultaat kan groter zijn dan jouw context window dus krijg jij een preview van de data terwijl de volledige data naast jouw antwoord wordt getoond in een pandas dataframe. Je hoeft je resultaat niet te tonen, enkel herkennen dat de functie succesvol. Jij krijgt een preview zodat als er vragen zijn jij die veranderen kan doorvoeren.
    Args:
        sql_query (str): De SQL-query om uit te voeren.
    Returns:
        
    Opmerking:
        Gebruik eerst de functies list_tables en describe_tables voor context.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            full_df = pd.DataFrame(result)
            st.session_state.data = full_df
            st.session_state.data.columns =[ x[0] for x in cursor.description ]
            return "Het volgende is een preview van data, de user krijgt de hele data te zien. Jij, de chatbot krijgt een deel omdat er anders het risico is om jou context window te overflowen. Vermeld in je antwoord dat jij een preview hebt van de data en de volledige data rechts van de chat te vinden is!" +  str(full_df.head(1))

@st.cache_resource
def load_tools():
    multiply_tool = FunctionTool.from_defaults(fn=multiply)
    add_tool = FunctionTool.from_defaults(fn=add)
    company_tool = FunctionTool.from_defaults(fn=company_api_call)
    companies_tool = FunctionTool.from_defaults(fn=companies_ids_api_call)
    tarief_tax_tool = FunctionTool.from_defaults(fn=has_tax_decreased_api_call)
    period_tool = FunctionTool.from_defaults(fn=period_id_fetcher)
    # account_tool = FunctionTool.from_defaults(fn=account_details)
    reconciliation_tool = FunctionTool.from_defaults(fn=reconciliation_api_call)
    list_tables_tool = FunctionTool.from_defaults(fn=list_tables)
    describe_tables_tool = FunctionTool.from_defaults(fn=describe_tables)
    load_data_tool = FunctionTool.from_defaults(fn=load_data)
    vergelijk_op_basis_van_tool = FunctionTool.from_defaults(vergelijk_op_basis_van)
    bereken_tool = FunctionTool.from_defaults(bereken)
    get_datum_tool = FunctionTool.from_defaults(get_date)
    voorafbetaling_tool = FunctionTool.from_defaults(fn=voorafbetaling)


    return [
        budget_tool,
        tarief_tax_tool,
        companies_tool,
        period_tool,
        company_tool,
        # EBITDA_tool,
        # list_tables_tool,
        # describe_tables_tool,
        load_data_tool,
        # balanstotaal_tool, eigen_vermogen_tool, handelswerkkapitaal_tool, bruto_marge_tool, omzet_tool, handelsvorderingen_tool, DSO_tool,
        # voorzieningen_tool, financiele_schuld_tool, liquide_middelen_tool, EBITDA_marge_tool, afschrijvingen_tool, EBIT_tool, netto_financiele_schuld_tool
        bereken_tool,
        vergelijk_op_basis_van_tool,
        get_datum_tool,
        voorafbetaling_tool
    ]




# def chart():
#     """Creates a chart based on the data. Use this tool when a user requests a chart"""
#     st.session_state.agent = True
#     return "Succesfully created chart"


# tools = load_tools()
list_tables_tool = FunctionTool.from_defaults(fn=list_tables)
describe_tables_tool = FunctionTool.from_defaults(fn=describe_tables)
load_data_tool = FunctionTool.from_defaults(fn=load_data)
voorafbetaling_tool = FunctionTool.from_defaults(fn=voorafbetaling)
# chart_tool = FunctionTool.from_defaults(fn=chart)

def create_agent():
    llm = OpenAI(model="gpt-4o", temperature=0)
    buffer = ChatMemoryBuffer(token_limit=300)
    agent = OpenAIAgent.from_tools(
        load_tools(), verbose=True, llm=llm, system_prompt=system_prompt, memory_cls=buffer
    )
    return agent


if "agent" not in st.session_state:
    st.session_state.agent = create_agent()
agent = st.session_state.agent


# CSS injection that makes the user input right-aligned
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

email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

if st.session_state.get("UUID") is None:
    st.session_state["UUID"] = uuid4().hex

if (
    st.session_state["authentication_status"] is None
    or st.session_state["authentication_status"] is False
):
    st.title("Welkom bij het Knowledge Center!")
    try:
        authenticator.login()
    except stauth.LoginError as e:
        st.error(e)

    if st.session_state["authentication_status"]:
        st.session_state["active_section"] = "Chatbot"

    elif st.session_state["authentication_status"] is False:
        st.error("Username/password is incorrect")
    elif st.session_state["authentication_status"] is None:
        st.warning("Please enter your username and password")
        st.session_state["active_section"] = "Username"


# if "username" not in st.session_state or "active_session" in st.session_state and st.session_state.active_session == "Username":
#     st.title("Welkom bij het Knowledge Center! 🎉")
#     with st.form("username_form"):
#         st.markdown(
#             "<p style='font-size:20px;'>Vul hieronder je mailadres in om van start te gaan met het Knowledge Center  </p>",
#             unsafe_allow_html=True
#         )
#         with st.container(border=True):
#             username = st.text_input("Hallo", placeholder="Emailadres", label_visibility="collapsed")
        
#         # Form submission button
#         submit_button = st.form_submit_button("Start Knowledge Center")

#         # Check if the form is submitted
#         if submit_button:
#             if re.match(email_regex, username):
#                 if username:
#                     # Set the username as username in session state
#                     st.session_state['username'] = username
#                     st.session_state["active_section"] = "Chatbot"
#                     st.success(f"Succesvol ingelogd")

#                     st.rerun()
#                 else:
#                     st.error("Ongeldig emailadres. Vul aub een geldig emailadres in.")
                
                
#             else:
#                 st.error("Vul aub een emailadres in.")

col1, col2, col3 = st.sidebar.columns([1, 6, 1])
col2.image("images/vgd2.webp")

firestore_string = st.secrets["FIRESTORE"]
firestore_cred = json.loads(firestore_string)
db = firestore.Client.from_service_account_info(firestore_cred)


def popup():
    if "username" not in st.session_state or st.session_state["username"] is None:
        st.toast("U moet inloggen voor deze functionaliteit.")


if "state_dict" not in st.session_state:
    st.session_state["state_dict"] = {}

with st.sidebar.container():
    sidecol1, sidecol2, sidecode3 = st.columns(3)

    chatbot_button = st.button(
        "Knowledge Center", use_container_width=True, on_click=popup
    )
    upload_button = st.button("Upload Files", use_container_width=True, on_click=popup)
    connecties = st.button("Connecties", use_container_width=True, on_click=popup)
    voorkeuren = st.button("Voorkeuren", use_container_width=True, on_click=popup)
    rapporten = st.button("Rapporten", use_container_width=True, on_click=popup)
    uitloggen = st.button("Log Uit", use_container_width=True, on_click=popup)


if st.secrets["PROD"] == "False" and "username" in st.session_state:
    if os.path.exists(f"analytics/{st.session_state.username}.json"):
        streamlit_analytics2.start_tracking(
            load_from_json=f"analytics/{st.session_state.username}.json"
        )
    else:
        streamlit_analytics2.main.reset_counts()
        streamlit_analytics2.start_tracking()

else:
    streamlit_analytics2.start_tracking()

if "active_section" not in st.session_state:
    st.session_state["active_section"] = "Username"

if (
    chatbot_button
    and "username" in st.session_state
    and st.session_state.username is not None

):
    st.session_state["active_section"] = "Chatbot"

if (
    upload_button
    and "username" in st.session_state
    and st.session_state.username is not None
):
    st.session_state["active_section"] = "Upload Files"

if (
    connecties
    and "username" in st.session_state
    and st.session_state.username is not None
):
    st.session_state["active_section"] = "Connecties"

if (
    voorkeuren
    and "username" in st.session_state
    and st.session_state.username is not None
):
    st.session_state["active_section"] = "Voorkeuren"

if (
    rapporten
    and "username" in st.session_state
    and st.session_state.username is not None
):
    st.session_state["active_section"] = "Rapporten"

if (
    uitloggen
    and "username" in st.session_state
    and st.session_state.username is not None
):
    st.session_state["active_section"] = "Uitloggen"


if st.session_state["active_section"] == "Chatbot":
    st.title("Knowledge Center")
    col1, col2 = st.columns([1,1])
    col2.dataframe(st.session_state.data, use_container_width=True, height=500)
    # if "chart"  in st.session_state:
    #     col2.line_chart(st.session_state.data)
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

    if "openai_model" not in st.session_state:
        st.session_state["openai_model"] = "gpt-4o"

    if "messages" not in st.session_state:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": "Hallo, hoe kan ik je helpen? Stel mij al je financiële vragen!",
            }
        ]
    colcon1 = col1.container(height=500)
    for message in st.session_state.messages:
        if message["role"] != "system":
            if message["role"] == "assistant":
                with colcon1.chat_message(message["role"], avatar="images/vgd_logo3.jpeg"):
                    st.markdown(message["content"])
            else:
                with colcon1.chat_message(message["role"]):
                    st.markdown(message["content"])
    try:
        prompt = st.chat_input("Stel hier je vraag!", key="real_chat_input")
        if prompt:
            st.session_state.messages.append({"role": "user", "content": prompt})
            with col1.container():
                with colcon1.chat_message("user"):
                    st.markdown(prompt)
            with col1.container():
                with colcon1.chat_message("assistant", avatar="images/vgd_logo3.jpeg"):
                    with st.spinner("Thinking..."):
                        try:
                            mess = agent.stream_chat(prompt)

                        except Exception as e:
                            response = "Sorry, there was an error processing your request. Please try again."
                            st.error("Error in agent response: " + str(e))
                    response = st.write_stream(mess.response_gen)
                    st.session_state.messages.append(
                        {"role": "assistant", "content": response}
                    )
                    time.sleep(0.5)
                    st.rerun()
            if st.session_state.messages[-1]["role"] == "user":
                with colcon1.chat_message("assistant", avatar="images/FINTRAX_EMBLEM_POS@2x_TRANSPARENT.png"):
                    with colcon1.spinner("Thinking..."):
                        try:
                            mess = agent.stream_chat(
                                st.session_state.messages[-1]["content"]
                            )
                        except Exception as e:
                            response = "Sorry, there was an error processing your request. Please try again."
                            st.error("Error in agent response: " + str(e))
                        response = st.write_stream(mess.response_gen)
                        st.session_state.messages.append(
                            {"role": "assistant", "content": response}
                        )
                        time.sleep(0.5)
                        st.rerun()


    except Exception as e:
        response = (
            "Sorry, there was an error processing your request. Please try again."
        )
        st.error("Error in agent response: " + str(e))


# If Upload Files is selected
elif st.session_state["active_section"] == "Upload Files":
    st.title("Upload Files")
    st.markdown("Deze functie is nog niet beschikbaar")
    uploaded_file = st.file_uploader(
        "Choose a file", type=["txt", "pdf", "docx", "csv"]
    )

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
    st.markdown(
        "Deze functie is nog niet beschikbaar, verbinden met de API gaat nog geen data doorgeven."
    )

    fin1, fin2, fin3 = st.columns(3)
    with fin1:
        with st.container(border=True):
            fincol1, fincol2 = st.columns(2)
            fincol1.image("images/silverfin-logo.png")
            fincol2.subheader("Silverfin")
            fincol2.markdown(
                """Naam: Fiduciaire ABC
                                ID: 561
                                mark@abcaccouting.be"""
            )
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
            fincol2.markdown(
                """Naam: FIDUCIAIRE ABC 
                                support@fintrax.io"""
            )
            verbindmy = st.button("Connecteer MyMinFin", use_container_width=True)
            if verbindmy:
                with st.spinner("Verbinden..."):
                    time.sleep(3)
                st.success("Verbonden!")


elif st.session_state["active_section"] == "Voorkeuren":
    st.title("Voorkeuren")
    st.markdown("Maak hier je standaardvragen of rapporten aan")
    # Initialize session state to store questions
    if "questions" not in st.session_state:
        st.session_state.questions = []

    # Function to add a new question input field
    def add_question():
        st.session_state.questions.append("")

    def remove_question():
        if st.session_state.questions != 0:
            st.session_state.questions = st.session_state.questions[:-1]

    add, rem, _ = st.columns([1, 1, 30])
    # Button to add a new question dynamically
    add.button("\+", on_click=add_question)
    rem.button("\-", on_click=remove_question)

    # Display the input fields for the questions dynamically
    for i, question in enumerate(st.session_state.questions):
        st.session_state.questions[i] = st.text_input(
            f"Question {i+1}:", value=question, key=f"question_{i}"
        )

    # Button to save the form
    if st.button("Opslaan"):
        st.success("Opgeslagen!")

elif st.session_state["active_section"] == "Rapporten":
    st.title("Rapporten")
    st.markdown("Deze functie is nog niet beschikbaar")
    if "questions" in st.session_state:
        st.dataframe({"Vragen": st.session_state.questions}, width=800)
        if st.button("Voer Uit"):
            time.sleep(3)
            st.success("Uitgevoerd!")

elif st.session_state["active_section"] == "Uitloggen":
    st.title("Welkom bij het Knowledge Center!")
    # log_out = st.button("Log uit")
    # if "username" in st.session_state and log_out:
    #     st.session_state.pop("username")
    #     st.toast("U bent uitgelogd")
    #     st.session_state.active_section = "Username"
    #     st.rerun()
    try:
        authenticator.login()
    except stauth.LoginError as e:
        st.error(e)

    if st.session_state["authentication_status"]:
        authenticator.logout()

    # elif st.session_state["authentication_status"] is False:
    #     st.error("Username/password is incorrect")
    # elif st.session_state["authentication_status"] is None:
    #     st.warning("Please enter your username and password")
    #     st.session_state["active_section"] = "Username"


if (
    st.secrets["PROD"] == "False"
    and "username" in st.session_state
    and "UUID" in st.session_state
):
    streamlit_analytics2.stop_tracking(
        save_to_json=f"analytics/{st.session_state.username}.json",
        unsafe_password=st.secrets["ANALYTICS_PWD"],
    )
    doc_ref = db.collection("new_users").document(str(st.session_state.username))

    analytics_data = pd.read_json(f"analytics/{st.session_state.username}.json").to_dict()
    analytics_data["chat"] = {
        st.session_state.get("UUID"): st.session_state.get("messages")
    }
    doc_ref.set(analytics_data, merge=True)
else:
    streamlit_analytics2.stop_tracking()
