from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, DatabaseError
import os

class DatabaseConnection():
    def __init__(self, logger=None, test_conn=True):
        self.logger = logger
        DB_URL = os.getenv("DB_URL")
        if not DB_URL:
            logger.error("DB_URL environment variable not set. Exiting.")
            exit(1)
        if test_conn:
            self.verify_database_connection()
        engine = create_engine(DB_URL, echo=False)
        self.Session = sessionmaker(bind=engine)

    def verify_database_connection(self):
        self.logger.info(f"testing Database connection")
        try:
            with self.Session() as session:
                result = session.execute(text("SELECT COUNT(*) FROM schedules"))
                count = result.scalar()
                self.logger.info(f"Database connection OK. Found {count} entries in schedules table.")
        except OperationalError as e:
            self.logger.error(f"Connection failed (will retry later): {e}")
        except DatabaseError as e:
            self.logger.error(f"Database error (table missing or permission issue?): {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during DB test: {e}")
