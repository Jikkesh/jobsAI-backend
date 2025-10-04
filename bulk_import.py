from datetime import datetime, timezone
from typing import List, Dict, Optional, Set, Tuple
from sqlalchemy import text
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from db import SessionLocal
from sqlalchemy.orm import Session

# Router setup
router = APIRouter()

# Pydantic models
class JobInput(BaseModel):
    category: str
    company_name: str
    job_role: str
    website_link: str
    state: Optional[str] = "Not specified"
    city: Optional[str] = "Not specified"
    experience: Optional[str] = "Not specified"
    qualification: Optional[str] = "Not specified"
    batch: Optional[str] = "Not specified"
    salary_package: Optional[str] = "Not specified"
    job_description: Optional[str] = "Not specified"
    key_responsibility: Optional[str] = "Not specified"
    about_company: Optional[str] = "Not specified"
    selection_process: Optional[str] = "Not specified"
    image: Optional[str] = "Not specified"
    posted_on: Optional[str] = None

class BulkJobsRequest(BaseModel):
    jobs: List[JobInput]

class BulkJobsResponse(BaseModel):
    success: bool
    total_jobs: int
    imported: int
    duplicates: int
    failed: int
    message: str

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Core import function
def import_jobs_bulk(jobs: List[JobInput], db: Session) -> Dict:
    """
    Import multiple jobs into the database with duplicate checking and validation.
    
    Args:
        jobs: List of job data to import
        db: Database session
        
    Returns:
        Dictionary with import statistics
    """
    
    def load_existing_jobs(category: str) -> Set[Tuple]:
        """Load existing jobs from database to check for duplicates"""
        try:
            query = text("""
                SELECT LOWER(TRIM(company_name)) as company_name, 
                       LOWER(TRIM(job_role)) as job_role,
                       LOWER(TRIM(website_link)) as website_link,
                       DATE(posted_on) as posted_date,
                       category
                FROM jobs
                WHERE company_name IS NOT NULL 
                AND job_role IS NOT NULL
                AND category = :category
            """)
            
            result = db.execute(query, {"category": category})
            existing = set()
            
            for row in result:
                job_key = (
                    row.company_name,
                    row.job_role,
                    row.website_link,
                    row.posted_date,
                    row.category
                )
                existing.add(job_key)
            
            return existing
            
        except Exception as e:
            print(f"Error loading existing jobs: {str(e)}")
            return set()
    
    def parse_posted_on(posted_on_str: Optional[str]) -> datetime:
        """Parse posted_on string to datetime object"""
        if not posted_on_str or posted_on_str == 'Not specified':
            return datetime.now(timezone.utc)

        date_formats = [
            '%Y-%m-%dT%H:%M:%S+00:00',  # ISO 8601 with timezone
            '%Y-%m-%dT%H:%M:%SZ',       # ISO 8601 UTC
            '%Y-%m-%dT%H:%M:%S',        # ISO 8601 without timezone
            '%Y-%m-%d %H:%M:%S',        # Standard datetime
            '%Y-%m-%d',                 # Date only
            '%d-%m-%Y',
            '%m/%d/%Y',
            '%d/%m/%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(posted_on_str, fmt)
            except ValueError:
                continue
        
        print(f"Warning: Could not parse date '{posted_on_str}', using current date")
        return datetime.now(timezone.utc)
    
    def clean_text_field(value: Optional[str]) -> str:
        """Clean text fields"""
        if value is None or value == '':
            return 'Not specified'
        return str(value).strip()
    
    def create_job_key(company_name: str, job_role: str, website_link: str, 
                      posted_on: datetime, category: str) -> Tuple:
        """Create a unique key for duplicate checking"""
        return (
            company_name.lower().strip(),
            job_role.lower().strip(),
            website_link.lower().strip(),
            posted_on.date(),
            category
        )
    
    # Statistics
    stats = {
        'total': len(jobs),
        'imported': 0,
        'duplicates': 0,
        'failed': 0
    }
    
    # Group jobs by category to load existing jobs efficiently
    jobs_by_category = {}
    for job in jobs:
        category = job.category
        if category not in jobs_by_category:
            jobs_by_category[category] = []
        jobs_by_category[category].append(job)
    
    try:
        # Process each category
        for category, category_jobs in jobs_by_category.items():
            # Load existing jobs for this category
            existing_jobs = load_existing_jobs(category)
            
            # Process each job
            for job in category_jobs:
                try:
                    # Parse and clean data
                    posted_on = parse_posted_on(job.posted_on)
                    company_name = clean_text_field(job.company_name)
                    job_role = clean_text_field(job.job_role)
                    website_link = clean_text_field(job.website_link)
                    
                    # Check for duplicates
                    job_key = create_job_key(company_name, job_role, website_link, 
                                            posted_on, category)
                    
                    if job_key in existing_jobs:
                        stats['duplicates'] += 1
                        continue
                    
                    # Prepare job data
                    job_data = {
                        'category': category,
                        'company_name': company_name,
                        'job_role': job_role,
                        'website_link': website_link,
                        'state': clean_text_field(job.state),
                        'city': clean_text_field(job.city),
                        'experience': clean_text_field(job.experience),
                        'qualification': clean_text_field(job.qualification),
                        'batch': clean_text_field(job.batch),
                        'salary_package': clean_text_field(job.salary_package),
                        'job_description': clean_text_field(job.job_description),
                        'key_responsibility': clean_text_field(job.key_responsibility),
                        'about_company': clean_text_field(job.about_company),
                        'selection_process': clean_text_field(job.selection_process),
                        'image': clean_text_field(job.image),
                        'posted_on': posted_on
                    }
                    
                    # Insert into database
                    insert_query = text("""
                        INSERT INTO jobs (
                            category, company_name, job_role, website_link, state, city,
                            experience, qualification, batch, salary_package, job_description,
                            key_responsibility, about_company, selection_process, image,
                            posted_on
                        ) VALUES (
                            :category, :company_name, :job_role, :website_link, :state, :city,
                            :experience, :qualification, :batch, :salary_package, :job_description,
                            :key_responsibility, :about_company, :selection_process, :image,
                            :posted_on
                        )
                    """)
                    
                    db.execute(insert_query, job_data)
                    stats['imported'] += 1
                    
                    # Add to existing jobs cache to prevent duplicates within same request
                    existing_jobs.add(job_key)
                    
                    # Commit periodically for better performance
                    if stats['imported'] % 50 == 0:
                        db.commit()
                
                except Exception as e:
                    print(f"Error processing job: {str(e)}")
                    stats['failed'] += 1
                    continue
        
        # Final commit
        db.commit()
        
    except Exception as e:
        db.rollback()
        raise Exception(f"Error during bulk import: {str(e)}")
    
    return stats