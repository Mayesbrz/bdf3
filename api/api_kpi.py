from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import pandas as pd
import sqlalchemy
import secrets

USERNAME = "amayas"  
PASSWORD = "amayas"  
security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, USERNAME)
    correct_password = secrets.compare_digest(credentials.password, PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )

MYSQL_URL = "mysql+pymysql://username:password@localhost:3309/accidents?charset=utf8mb4"
engine = sqlalchemy.create_engine(MYSQL_URL)

app = FastAPI(title="KPI Accidents API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/kpi/global")
def kpi_global(dep=Depends(verify_credentials)):
    with engine.connect() as conn:
        total = pd.read_sql("SELECT SUM(accident_count) as total FROM gold_accidents_by_region", conn)["total"][0]
        avg = pd.read_sql("SELECT AVG(avg_severity) as avg FROM gold_accidents_by_region", conn)["avg"][0]
    return {"total_accidents": int(total), "avg_severity": float(avg)}

@app.get("/kpi/top_regions")
def kpi_top_regions(dep=Depends(verify_credentials)):
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT State, County, SUM(accident_count) as accidents
            FROM gold_accidents_by_region
            GROUP BY State, County
            ORDER BY accidents DESC
            LIMIT 5
        """, conn)
    return df.to_dict(orient="records")

@app.get("/kpi/top_cities")
def kpi_top_cities(dep=Depends(verify_credentials)):
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT City, SUM(accident_count) as accidents
            FROM gold_accidents_by_region
            GROUP BY City
            ORDER BY accidents DESC
            LIMIT 5
        """, conn)
    return df.to_dict(orient="records")

@app.get("/kpi/by_hour")
def kpi_by_hour(dep=Depends(verify_credentials)):
    with engine.connect() as conn:
        df = pd.read_sql("SELECT hour, SUM(accident_count) as accidents FROM gold_accidents_by_hour_severity GROUP BY hour ORDER BY hour", conn)
    return df.to_dict(orient="records")

@app.get("/kpi/by_severity")
def kpi_by_severity(dep=Depends(verify_credentials)):
    with engine.connect() as conn:
        df = pd.read_sql("SELECT Severity, SUM(accident_count) as accidents FROM gold_accidents_by_hour_severity GROUP BY Severity ORDER BY Severity", conn)
    return df.to_dict(orient="records")

@app.get("/kpi/by_weather")
def kpi_by_weather(dep=Depends(verify_credentials)):
    with engine.connect() as conn:
        df = pd.read_sql("SELECT Weather_Condition, SUM(accident_count) as accidents FROM gold_accidents_by_weather GROUP BY Weather_Condition ORDER BY accidents DESC LIMIT 10", conn)
    return df.to_dict(orient="records")

@app.get("/kpi/max_hour")
def kpi_max_hour(dep=Depends(verify_credentials)):
    with engine.connect() as conn:
        df = pd.read_sql("SELECT hour, SUM(accident_count) as accidents FROM gold_accidents_by_hour_severity GROUP BY hour ORDER BY hour", conn)
    max_hour = df.loc[df["accidents"].idxmax(), "hour"]
    return {"max_hour": max_hour}
