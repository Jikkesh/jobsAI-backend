import io
from mimetypes import guess_type
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, Response, UploadFile
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
from bulk_import import BulkJobsRequest, BulkJobsResponse, JobInput, import_jobs_bulk
from db import get_db
from models import Job
from schemas import CategoryResponse, JobCreate, JobOut, JobResponse, JobUpdate
from typing import Dict, List
from sqlalchemy.exc import OperationalError

import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["Jobs"])

def get_image_url(job: Job, request: Request) -> str:
    if job.image:
        return f"{request.base_url}images/{job.image}"
    return ""

def job_to_response(job: Job, request: Request) -> JobResponse:
    """
    Converts a Job ORM instance to a JobResponse schema.
    Also attaches the image URL if available.
    """
    job_response = JobResponse.from_orm(job)
    job_response.image_url = get_image_url(job, request)
    return job_response

# -------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------

@router.post("/", response_model=JobResponse, status_code=201)
def create_job(
    request: Request,
    category: str = Form(...),
    company_name: str = Form(...),
    job_role: str = Form(...),
    website_link: str = Form(None),
    state: str = Form(...),
    city: str = Form(...),
    experience: str = Form(...),
    qualification: str = Form(...),
    batch: str = Form(None),
    salary_package: str = Form(None),
    job_description: str = Form(...),
    key_responsibility: str = Form(None),
    about_company: str = Form(None),
    selection_process: str = Form(None),
    image: str = Form(None),
    db: Session = Depends(get_db)
):
    """
    Create a new job entry with basic validation.
    """
    # Validate required fields are not empty
    if not category or not category.strip():
        raise HTTPException(status_code=400, detail="Category is required")
    if not company_name or not company_name.strip():
        raise HTTPException(status_code=400, detail="Company name is required")
    if not job_role or not job_role.strip():
        raise HTTPException(status_code=400, detail="Job role is required")
    if not state or not state.strip():
        raise HTTPException(status_code=400, detail="State is required")
    if not city or not city.strip():
        raise HTTPException(status_code=400, detail="City is required")
    if website_link and not website_link.strip():
        raise HTTPException(status_code=400, detail="Website link cannot be empty if provided")

    if not qualification or not qualification.strip():
        raise HTTPException(status_code=400, detail="Qualification is required")
    if not job_description or not job_description.strip():
        raise HTTPException(status_code=400, detail="Job description is required")
    if not key_responsibility or not key_responsibility.strip():
        raise HTTPException(status_code=400, detail="Key responsibility is required")
    if not about_company or not about_company.strip():
        raise HTTPException(status_code=400, detail="About company is required")
    if not selection_process or not selection_process.strip():
        raise HTTPException(status_code=400, detail="Selection process is required")

    
    try:
        # Create new job instance
        new_job = Job(
            category=category,
            company_name=company_name,
            job_role=job_role,
            website_link=website_link,
            state=state,
            city=city,
            experience=experience,
            qualification=qualification,
            batch=batch,
            salary_package=salary_package,
            job_description=job_description,
            key_responsibility=key_responsibility,
            about_company=about_company,
            selection_process=selection_process,
            image=image
        )
        
        db.add(new_job)
        db.commit()
        db.refresh(new_job)
        
        return job_to_response(new_job, request)
    
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create job: {e}")
        raise HTTPException(status_code=500, detail="Failed to create job")
    
# API Endpoint
@router.post("/api/jobs/bulk-import-csv", response_model=BulkJobsResponse)
async def create_jobs_bulk_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Bulk import jobs from CSV file.
    
    Expected CSV columns:
    - category, company_name, job_role, website_link, state, city,
      experience, qualification, batch, salary_package, job_description,
      key_responsibility, about_company, selection_process, image, posted_on
    
    Returns statistics about the import process.
    """
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Validate required columns
        required_columns = ['category', 'company_name', 'job_role', 'website_link']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400, 
                detail=f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        # Convert DataFrame to list of JobInput objects
        jobs = []
        for _, row in df.iterrows():
            job_data = {
                'category': str(row.get('category', '')),
                'company_name': str(row.get('company_name', '')),
                'job_role': str(row.get('job_role', '')),
                'website_link': str(row.get('website_link', '')),
                'state': str(row.get('state', 'Not specified')),
                'city': str(row.get('city', 'Not specified')),
                'experience': str(row.get('experience', 'Not specified')),
                'qualification': str(row.get('qualification', 'Not specified')),
                'batch': str(row.get('batch', 'Not specified')),
                'salary_package': str(row.get('salary_package', 'Not specified')),
                'job_description': str(row.get('job_description', 'Not specified')),
                'key_responsibility': str(row.get('key_responsibility', 'Not specified')),
                'about_company': str(row.get('about_company', 'Not specified')),
                'selection_process': str(row.get('selection_process', 'Not specified')),
                'image': str(row.get('image', 'Not specified')),
                'posted_on': str(row.get('posted_on', None)) if pd.notna(row.get('posted_on')) else None
            }
            
            # Handle NaN values
            for key, value in job_data.items():
                if value == 'nan' or pd.isna(value):
                    job_data[key] = 'Not specified' if key != 'posted_on' else None
            
            jobs.append(JobInput(**job_data))
        
        if not jobs:
            raise HTTPException(status_code=400, detail="No valid jobs found in CSV")
        
        # Perform the import
        stats = import_jobs_bulk(jobs, db)
        
        # Prepare response message
        message = (
            f"Import completed: {stats['imported']} new jobs imported, "
            f"{stats['duplicates']} duplicates skipped, "
            f"{stats['failed']} failed"
        )
        
        return BulkJobsResponse(
            success=True,
            total_jobs=stats['total'],
            imported=stats['imported'],
            duplicates=stats['duplicates'],
            failed=stats['failed'],
            message=message
        )
    
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty")
    except pd.errors.ParserError as e:
        raise HTTPException(status_code=400, detail=f"CSV parsing error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in bulk CSV import: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

@router.get("/trending", response_model=List[JobResponse])
def get_trending_jobs(
    request: Request,
    n: int = Query(5, ge=1, le=50, description="Number of random remote jobs to return (1-50)"),
    db: Session = Depends(get_db)
):

    try:
        # Get N random remote jobs
        jobs = (
            db.query(Job)
            .filter(Job.category == "Remote")
            .order_by(func.random())  # Use func.random() for PostgreSQL/SQLite
            .limit(n)
            .all()
        )
        
        if not jobs:
            # If no remote jobs found, return empty list
            return []
        
        return [job_to_response(job, request) for job in jobs]
    
    except OperationalError as e:
        logger.warning(f"Database connection dropped, retrying query: {e}")
        db.rollback()  # rollback broken transaction
        try:
            # Retry logic
            jobs = (
                db.query(Job)
                .filter(Job.category == "Remote")
                .order_by(func.random())
                .limit(n)
                .all()
            )
            return [job_to_response(job, request) for job in jobs]
        except Exception as e2:
            logger.error(f"Retry failed: {e2}")
            raise HTTPException(status_code=500, detail="Database connection error")

@router.get("/latest", response_model=Dict[str, List[JobResponse]])
def get_latest_jobs(request: Request, db: Session = Depends(get_db)):
    """
    Retrieve the 2 latest jobs from each category (Fresher, Internship, Remote, Experienced).
    
    Returns:
        Dict with category names as keys and lists of JobResponse objects as values.
    """
    categories = ["Fresher", "Internship", "Remote", "Experienced"]
    result = {}
    
    try:
        for category in categories:
            # Get 2 latest jobs for each category
            jobs = (
                db.query(Job)
                .filter(Job.category == category)
                .order_by(Job.posted_on.desc())
                .limit(2)
                .all()
            )
            result[category] = [job_to_response(job, request) for job in jobs]
        
        return result
    
    except OperationalError as e:
        logger.warning(f"Database connection dropped, retrying query: {e}")
        db.rollback()  # rollback broken transaction
        try:
            # Retry logic
            result = {}
            for category in categories:
                jobs = (
                    db.query(Job)
                    .filter(Job.category == category)
                    .order_by(Job.posted_on.desc())
                    .limit(2)
                    .all()
                )
                result[category] = [job_to_response(job, request) for job in jobs]
            
            return result
        except Exception as e2:
            logger.error(f"Retry failed: {e2}")
            raise HTTPException(status_code=500, detail="Database connection error")

@router.get("/category/{category}", response_model=dict)
def get_jobs_by_category(
    request: Request,
    category: str,
    page: int = Query(1, alias="currentPage", ge=1),
    page_size: int = Query(10, alias="pageSize", ge=1),
    db: Session = Depends(get_db),
):
    """
    Retrieve paginated jobs for a given category with retry on connection drop.
    """
    try:
        return _fetch_jobs_by_category(request, category, page, page_size, db)

    except OperationalError as e:
        logger.warning(f"Database connection dropped, retrying query: {e}")
        db.rollback()  # rollback broken transaction
        try:
            return _fetch_jobs_by_category(request, category, page, page_size, db)
        except Exception as e2:
            logger.error(f"Retry failed: {e2}")
            raise HTTPException(status_code=500, detail="Database connection error")


def _fetch_jobs_by_category(request, category, page, page_size, db):
    temp = None
    if category.lower() == "ai":
        temp = "AI"
    else:
        temp = category.title()
    query = (
        db.query(Job)
        .filter(Job.category == temp)
        .order_by(Job.posted_on.desc())
    )
    total_count = query.count()
    jobs = query.offset((page - 1) * page_size).limit(page_size).all()

    return {
        "jobs": [job_to_response(job, request) for job in jobs],
        "totalCount": total_count
    }

#Get a Job by ID
@router.get("/{job_slug}", response_model=JobResponse)
def get_job(job_slug: str, request: Request, db: Session = Depends(get_db)):
    """
    Retrieve a specific job by its slug.

    Raises a 404 error if the job does not exist.
    """
    job = db.query(Job).filter(Job.job_slug == job_slug).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_to_response(job, request)

@router.get("/", response_model=List[JobResponse])
def get_jobs(request: Request, db: Session = Depends(get_db)):
    """
    Retrieve all jobs from the database.
    """
    jobs = db.query(Job).order_by(Job.posted_on.asc()).all()
    return [job_to_response(job, request) for job in jobs]

@router.put("/{job_id}", response_model=JobOut)
async def update_job(
    request: Request,
    job_id: int,
    category: str = Form(...),
    company_name: str = Form(...),
    job_role: str = Form(...),
    website_link: str = Form(None),
    state: str = Form(...),
    city: str = Form(...),
    experience: str = Form(...),
    qualification: str = Form(...),
    batch: str = Form(None),
    salary_package: str = Form(None),
    job_description: str = Form(...),
    key_responsibility: str = Form(None),
    about_company: str = Form(None),
    selection_process: str = Form(None),
    db: Session = Depends(get_db),
    image: UploadFile = File(None)
):
    """
    Update an existing job entry by its ID.
    """
    db_job = db.query(Job).filter(Job.id == job_id).first()
    if not db_job:
        raise HTTPException(status_code=404, detail="Job not found")

    db_job.category = category  # type: ignore
    db_job.company_name = company_name  # type: ignore
    db_job.job_role = job_role  # type: ignore
    db_job.website_link = website_link  # type: ignore
    db_job.state = state  # type: ignore
    db_job.city = city  # type: ignore
    db_job.experience = experience  # type: ignore
    db_job.qualification = qualification  # type: ignore
    db_job.batch = batch  # type: ignore
    db_job.salary_package = salary_package  # type: ignore
    db_job.job_description = job_description  # type: ignore
    db_job.key_responsibility = key_responsibility  # type: ignore
    db_job.about_company = about_company  # type: ignore
    db_job.selection_process = selection_process  # type: ignore

    if image:
        db_job.image = await image.read()  # type: ignore
        db_job.image_filename = image.filename

    db.commit()
    db.refresh(db_job)
    return job_to_response(db_job, request)

@router.delete("/{job_id}", response_model=JobOut)
def delete_job(job_id: int, request: Request, db: Session = Depends(get_db)):
    """
    Delete a job entry identified by its ID.
    """
    db_job = db.query(Job).filter(Job.id == job_id).first()
    if not db_job:
        raise HTTPException(status_code=404, detail="Job not found")
    db.delete(db_job)
    db.commit()
    return job_to_response(db_job, request)