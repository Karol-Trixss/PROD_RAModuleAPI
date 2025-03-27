from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import json
import pymssql  # Changed from pyodbc to pymssql
import logging
import time
from contextlib import contextmanager
from tqdm import tqdm
import pandas as pd
import decimal
from datetime import datetime, date
import os
from fastapi.responses import HTMLResponse
from functools import lru_cache
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Cache configuration
CACHE_SIZE = 128
CACHE_TTL = 3600  # 1 hour in seconds

app = FastAPI(title="RAF Calculator API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class Membership(BaseModel):
    MemberID: str
    DOB: str
    Gender: str
    RAType: str
    Hospice: str
    LTIMCAID: str
    NEMCAID: str
    OREC: str

    class Config:
        extra = "forbid"

class Diagnosis(BaseModel):
    MemberID: str
    FromDOS: str
    ThruDOS: str
    DxCode: str
    QualificationFlag: int
    UnqualificationReason: str

class ProcessDataRequest(BaseModel):
    dos_year: int
    memberships: List[Membership]
    diagnoses: List[Diagnosis]

def get_db_connection():
    """Establish a connection to the database."""
    try:
        conn = pymssql.connect(
            server='10.10.1.4',
            database='RAModuleQA',
            user='etl_user',
            password='etl_user',
            port='1433',
            charset='UTF-8',
            timeout=30
        )
        logger.info("Database connection successful")
        return conn
    except pymssql.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

@contextmanager
def get_db_cursor():
    """Context manager for database connections."""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        yield cursor
        conn.commit()
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

def create_temp_tables(cursor):
    """Create temporary tables for both membership and diagnosis data."""
    cursor.execute("""
    IF OBJECT_ID('tempdb..#TempMembership') IS NOT NULL
        DROP TABLE #TempMembership;
    IF OBJECT_ID('tempdb..#TempDiagnosis') IS NOT NULL
        DROP TABLE #TempDiagnosis;
 
    CREATE TABLE #TempMembership (
        MemberID VARCHAR(50) NOT NULL,
        BirthDate DATE NOT NULL,
        Gender VARCHAR(1) NULL,
        RAType VARCHAR(10) NULL,
        Hospice VARCHAR(1) NULL,
        LTIMCAID VARCHAR(1) NULL,
        NEMCAID VARCHAR(1) NULL,
        OREC VARCHAR(1) NULL
    );
 
    CREATE TABLE #TempDiagnosis (
        MemberID VARCHAR(50) NOT NULL,
        FromDOS DATE NOT NULL,
        ThruDOS DATE NOT NULL,
        DxCode VARCHAR(20) NOT NULL,
        QualificationFlag int NOT NULL,
	    UnqualificationReason VARCHAR(20)
                   
    );
    """)

@lru_cache(maxsize=CACHE_SIZE)
def process_data_with_sp_cached(dos_year: int, memberships_tuple: tuple, diagnoses_tuple: tuple):
    """Cached version of the data processing function."""
    try:
        memberships = [dict(m) for m in memberships_tuple]
        diagnoses = [dict(d) for d in diagnoses_tuple]
        with get_db_cursor() as cursor:
            return process_data_with_sp(cursor, dos_year, memberships, diagnoses)
    except Exception as e:
        logger.error(f"Cache processing error: {str(e)}")
        raise

def process_data_with_sp(cursor, dos_year, memberships, diagnoses):
    """Process data using the stored procedure."""
    try:
        create_temp_tables(cursor)
        logger.info('Temp tables created successfully')
 
        df_members = pd.DataFrame(memberships)
        df_members = df_members.rename(columns={'DOB': 'BirthDate'})
        total_members = len(df_members)
        batch_size = 1000
 
        logger.info("Inserting membership data...")
        for i in tqdm(range(0, total_members, batch_size), desc="Processing members"):
            batch = df_members.iloc[i:i+batch_size]
            for _, row in batch.iterrows():
                cursor.execute("""
                    INSERT INTO #TempMembership 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(row.MemberID),
                    str(row.BirthDate),
                    str(row.Gender),
                    str(row.RAType),
                    str(row.Hospice),
                    str(row.get('LTIMCAID', 'N')),
                    str(row.get('NEMCAID', 'N')),
                    str(row.OREC)
                ))
 
        df_diag = pd.DataFrame(diagnoses)
        total_diag = len(df_diag)
 
        logger.info("Inserting diagnosis data...")
        for i in tqdm(range(0, total_diag, batch_size), desc="Processing diagnoses"):
            batch = df_diag.iloc[i:i+batch_size]
            for _, row in batch.iterrows():
                cursor.execute("""
                    INSERT INTO #TempDiagnosis 
                    VALUES (%s, %s, %s, %s,%s,%s)
                """, (
                    str(row.MemberID),
                    str(row.FromDOS),
                    str(row.ThruDOS),
                    str(row.DxCode),
                    int(row.QualificationFlag),
                    str(row.UnqualificationReason)
                ))
 
        logger.info('Executing stored procedure...')
        cursor.execute("""
            DECLARE @PmtYear INT = %s;
            Declare @Membership as InputMembership_PartC
            Declare @DxTable as [InputDiagnosisSuspect]
            
            INSERT INTO @Membership (
                MemberID, BirthDate, Gender, RAType, 
                Hospice, LTIMCAID, NEMCAID, OREC
            )
            SELECT 
                MemberID, BirthDate, Gender, RAType,
                Hospice, LTIMCAID, NEMCAID, OREC
            FROM #TempMembership;
            
            INSERT INTO @DxTable (MemberID, FromDOS, ThruDOS, DxCode,QualificationFlag,UnqualificationReason)
            SELECT MemberID, FromDOS, ThruDOS, DxCode,QualificationFlag,UnqualificationReason
            FROM #TempDiagnosis;
   

                       
 
            EXEC dbo.sp_RS_Medicare_PartC_Outer_Suspect @PmtYear, @Membership, @DxTable,2;
        """, (dos_year,))
        
        results = cursor.fetchall()
        logger.info(f"Retrieved {len(results)} records from stored procedure")
        return results
 
    except Exception as e:
        logger.error(f"Error in process_data_with_sp: {str(e)}")
        raise

@app.get("/")
async def root():
    return {"message": "Welcome to RAF Calculator API"}

@app.post("/process_data")
async def process_data(request: ProcessDataRequest):
    """API endpoint to handle data processing with caching."""
    try:
        logger.info(f"Processing data for {len(request.memberships)} members and {len(request.diagnoses)} diagnoses")
        memberships_dict = [membership.model_dump() for membership in request.memberships]
        diagnoses_dict = [diagnosis.model_dump() for diagnosis in request.diagnoses]
        memberships_tuple = tuple(tuple(sorted(m.items())) for m in memberships_dict)
        diagnoses_tuple = tuple(tuple(sorted(d.items())) for d in diagnoses_dict)
        
        try:
            results = process_data_with_sp_cached(
                request.dos_year,
                memberships_tuple,
                diagnoses_tuple
            )
            cache_status = "Cache hit"
        except Exception as e:
            logger.error(f"Cache error: {str(e)}")
            process_data_with_sp_cached.cache_clear()
            results = process_data_with_sp_cached(
                request.dos_year,
                memberships_tuple,
                diagnoses_tuple
            )
            cache_status = "Cache miss"
            
        response_data = {
            'status': 'success',
            'message': 'Data processed successfully',
            'cache_status': cache_status,
            'count': len(results),
            'results': results,
            'timestamp': datetime.now().isoformat()
        }
        logger.info(f"Successfully processed {len(results)} records ({cache_status})")
        return response_data
 
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error: {error_message}")
        raise HTTPException(
            status_code=500,
            detail={
                'status': 'error',
                'message': 'Internal server error',
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            }
        )

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == '__main__':
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host='0.0.0.0', port=port)
