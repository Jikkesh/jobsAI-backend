import base64
from datetime import datetime
from pydantic import BaseModel, EmailStr, validator
from typing import List, Optional
from models import Job


# Base schema containing common job fields.
class JobBase(BaseModel):
    category: str
    company_name: str
    job_role: str
    website_link: Optional[str] = None
    state: str
    city: str
    # 'experience' is now a string field with predefined choices.
    experience: Optional[str]  # Allowed values: "Fresher", "1-3 years", "3-5 years"
    qualification: str
    batch: Optional[str] = None
    salary_package: Optional[str] = None
    job_description: str
    key_responsibility: Optional[str] = None
    about_company: Optional[str] = None
    selection_process: Optional[str] = None
    image: Optional[bytes] = None
    
#Job Schema
class JobCreate(JobBase):
    """Schema for creating a new job."""
    pass

class JobResponse(BaseModel):
    id: int
    category: str
    company_name: str
    job_role: str
    website_link: str
    state: str
    city: str
    experience: Optional[str]  = None
    is_fresher: bool = False
    qualification: str
    batch: str
    salary_package: str
    job_description: str
    key_responsibility: str
    about_company: str
    selection_process: str
    image_url: Optional[str] = None
    posted_on: datetime
    job_slug: str

    class Config:
        from_attributes = True


class JobUpdate(BaseModel):
    """Schema for updating job fields. All fields are optional."""
    category: Optional[str] = None
    company_name: Optional[str] = None
    job_role: Optional[str] = None
    website_link: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    experience: Optional[str] = None
    qualification: Optional[str] = None
    batch: Optional[str] = None
    salary_package: Optional[str] = None
    job_description: Optional[str] = None
    key_responsibility: Optional[str] = None
    about_company: Optional[str] = None
    selection_process: Optional[str] = None
    image: Optional[bytes] = None

class JobOut(JobBase):
    """Schema for returning job details."""
    id: int
    posted_on: datetime
    expiry_date: Optional[datetime] = None

    class Config:
        orm_mode = True
        

class CategoryResponse(BaseModel):
    category: str
    jobs_data: List[JobResponse]
    
    class Config:
        from_attributes = True

#User Schema
class UserCreate(BaseModel):
    name: str
    email: EmailStr
    password: str
    location: str

class UserResponse(BaseModel):
    id: int
    name: str
    email: EmailStr
    location: str

    class Config:
        orm_mode = True
