from sqlalchemy import Column, Integer, LargeBinary, String, Text, Boolean, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from db import Base
from datetime import datetime, timedelta

class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    category = Column(String, index=True, nullable=False)  # Fresher, Internship, Remote, Experienced
    company_name = Column(String, index=True, nullable=False)
    job_role = Column(String, index=True, nullable=False)
    website_link = Column(String, nullable=True)
    state = Column(String, nullable=False)
    city = Column(String, nullable=False)
    experience = Column(String, nullable=True)
    qualification = Column(Text, nullable=False)
    batch = Column(String, nullable=True)
    salary_package = Column(String, nullable=True)
    job_description = Column(Text, nullable=False)
    key_responsibility = Column(Text, nullable=True)
    about_company = Column(Text, nullable=True)
    #job_type = Column(String, nullable=False)
    selection_process = Column(Text, nullable=True)
    image = Column(String, nullable=True)
    posted_on = Column(DateTime, nullable=False)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    email = Column(String, unique=True, index=True)
    password = Column(String)
    location = Column(String)