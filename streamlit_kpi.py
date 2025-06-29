import streamlit as st
import pandas as pd
import sqlalchemy


MYSQL_URL = "mysql+pymysql://username:password@localhost:3309/accidents?charset=utf8mb4"
engine = sqlalchemy.create_engine(MYSQL_URL)

st.set_page_config(page_title="KPI Accidents USA", layout="wide")
st.title("🚦 Dashboard KPI Accidents USA")


with engine.connect() as conn:
    total_accidents = pd.read_sql("SELECT SUM(accident_count) as total FROM gold_accidents_by_region", conn)["total"][0]
    avg_severity = pd.read_sql("SELECT AVG(avg_severity) as avg FROM gold_accidents_by_region", conn)["avg"][0]

col1, col2, col3 = st.columns(3)
col1.metric("Nombre total d'accidents", f"{total_accidents:,}")
col2.metric("Gravité moyenne", f"{avg_severity:.2f}")


with engine.connect() as conn:
    by_hour = pd.read_sql("SELECT hour, SUM(accident_count) as accidents FROM gold_accidents_by_hour_severity GROUP BY hour ORDER BY hour", conn)
max_hour = by_hour.loc[by_hour["accidents"].idxmax(), "hour"]
col3.metric("Heure la plus accidentogène", max_hour, help="L'heure où le nombre d'accidents est le plus élevé. Permet d'optimiser la présence des secours.")



with engine.connect() as conn:
    top_regions = pd.read_sql("""
        SELECT State, County, SUM(accident_count) as accidents
        FROM gold_accidents_by_region
        GROUP BY State, County
        ORDER BY accidents DESC
        LIMIT 5
    """, conn)

top_regions["Region"] = top_regions["State"] + " - " + top_regions["County"]
st.subheader("Top 5 régions à risque (par nombre d'accidents)")
st.caption("Ces régions (État + Comté) concentrent le plus d'accidents. Cela permet de cibler les zones prioritaires pour la prévention et l'allocation de ressources.")
st.bar_chart(top_regions.set_index("Region")["accidents"])


with engine.connect() as conn:
    top_cities = pd.read_sql("""
        SELECT City, SUM(accident_count) as accidents
        FROM gold_accidents_by_region
        GROUP BY City
        ORDER BY accidents DESC
        LIMIT 5
    """, conn)

st.subheader("Top 5 villes à risque (par nombre d'accidents)")
st.caption("Les villes les plus accidentogènes sont des cibles clés pour des campagnes de sensibilisation locales.")
st.bar_chart(top_cities.set_index("City"))


with engine.connect() as conn:
    by_hour = pd.read_sql("SELECT hour, SUM(accident_count) as accidents FROM gold_accidents_by_hour_severity GROUP BY hour ORDER BY hour", conn)

st.subheader("Répartition des accidents par heure")
st.caption("Identifier les pics horaires permet d'adapter la présence des secours et de mener des actions de prévention ciblées (ex : heures de pointe).")
st.line_chart(by_hour.set_index("hour"))


with engine.connect() as conn:
    by_severity = pd.read_sql("SELECT Severity, SUM(accident_count) as accidents FROM gold_accidents_by_hour_severity GROUP BY Severity ORDER BY Severity", conn)

st.subheader("Répartition des accidents par gravité")
st.caption("La gravité des accidents guide la priorisation des interventions et la planification des moyens médicaux.")
st.bar_chart(by_severity.set_index("Severity"))


with engine.connect() as conn:
    by_weather = pd.read_sql("SELECT Weather_Condition, SUM(accident_count) as accidents FROM gold_accidents_by_weather GROUP BY Weather_Condition ORDER BY accidents DESC LIMIT 10", conn)

st.subheader("Top 10 conditions météo accidentogènes")
st.caption("Comprendre l'impact des conditions météo permet d'anticiper les risques et d'adapter la communication lors d'événements climatiques.")
st.bar_chart(by_weather.set_index("Weather_Condition"))


st.markdown("---")
st.info("**Business value :** Ce dashboard permet d'identifier les zones, villes, horaires, gravités et conditions météo les plus à risque pour orienter les actions de prévention, prioriser les interventions et optimiser les ressources d'intervention.")
