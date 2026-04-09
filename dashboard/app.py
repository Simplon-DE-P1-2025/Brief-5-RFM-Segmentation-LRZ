import os

import streamlit as st


def mask_connection_string(value: str) -> str:
    if not value:
        return "Non definie"

    if "://" not in value or "@" not in value:
        return value

    scheme, remainder = value.split("://", 1)
    credentials, host_part = remainder.split("@", 1)

    if ":" not in credentials:
        return f"{scheme}://***@{host_part}"

    username, _password = credentials.split(":", 1)
    return f"{scheme}://{username}:***@{host_part}"


st.set_page_config(
    page_title="RFM Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title("RFM Dashboard")
st.caption("Squelette initial du dashboard Streamlit.")

st.info(
    "Cette application est un point de depart. "
    "Les vues metier et les graphiques seront ajoutes ensuite."
)

st.subheader("Etat du projet")
st.write(
    "Le service dashboard est maintenant present dans le repository avec une "
    "application minimale qui demarre dans Docker."
)

st.subheader("Connexion attendue")
st.code(mask_connection_string(os.getenv("RFM_DB_CONN", "")))

col1, col2 = st.columns(2)

with col1:
    st.markdown("### A venir")
    st.write("- Vue d'ensemble RFM")
    st.write("- Filtres par segment")
    st.write("- Exploration client")

with col2:
    st.markdown("### Notes techniques")
    st.write("- Framework: Streamlit")
    st.write("- Port expose: 8501")
    st.write("- Variable d'environnement: RFM_DB_CONN")
