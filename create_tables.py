from tv_detection_common.models import Base
from sqlalchemy import create_engine
import os

DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
Base.metadata.create_all(bind=engine)
print("Tables created or already exist.")
