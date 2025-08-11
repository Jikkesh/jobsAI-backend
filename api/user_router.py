from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import smtplib
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from db import get_db
from models import User
from schemas import UserCreate, UserResponse
from typing import List

from dotenv import load_dotenv
load_dotenv() 

router = APIRouter(prefix="/users", tags=["Users"])

class ContactForm(BaseModel):
    name: str
    email: str
    message: str


@router.post("/contact")
async def send_contact_email(form_data: ContactForm):
    # Email configuration (ensure SMTP_USER is jikkekumar98@gmail.com)
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
    SMTP_USER = os.getenv("SMTP_USER", "jikkekumar98@gmail.com")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "your-app-password")

    html_body = f"""
<html>
  <head>
    <style>
      body {{
        background-color: #f0f4f8;
        font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
        margin: 0;
        padding: 0;
      }}
      .email-container {{
        background-color: #ffffff;
        max-width: 600px;
        margin: 40px auto;
        border: 1px solid #dcdcdc;
        border-radius: 8px;
        overflow: hidden;
      }}
      .email-header {{
        background-color: #007bff;
        padding: 20px;
        text-align: center;
        color: #ffffff;
      }}
      .email-content {{
        padding: 20px;
        color: #333333;
      }}
      .email-content p {{
        line-height: 1.6;
      }}
      .email-footer {{
        background-color: #f0f4f8;
        padding: 10px;
        text-align: center;
        font-size: 12px;
        color: #999999;
      }}
      .highlight {{
        background-color: #e2f0ff;
        padding: 2px 6px;
        border-radius: 4px;
      }}
    </style>
  </head>
  <body>
    <div class="email-container">
      <div class="email-header">
        <h2>New Contact Form Submission</h2>
      </div>
      <div class="email-content">
        <p><strong>Name:</strong> <span class="highlight">{form_data.name}</span></p>
        <p><strong>Email:</strong> <span class="highlight">{form_data.email}</span></p>
        <p><strong>Message:</strong></p>
        <p class="highlight">{form_data.message}</p>
      </div>
      <div class="email-footer">
        <p>Jobs AI - Empowering Your Career Journey</p>
      </div>
    </div>
  </body>
</html>
"""


    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = "jobsai2001@gmail.com"
    msg["Subject"] = "New Contact Form Submission"
    
    msg["X-Priority"] = "1"
    msg["Priority"] = "urgent"
    msg["Importance"] = "high"

    # Attach the HTML body to the email
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, ["josephdrusela@gmail.com"], msg.as_string())
        return {"message": "Email sent successfully"}
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send email: {str(e)}"
        )


@router.post("/", response_model=UserResponse)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = User(**user.dict())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

@router.get("/", response_model=List[UserResponse])
def get_users(db: Session = Depends(get_db)):
    return db.query(User).all()

@router.get("/{user_id}", response_model=UserResponse)
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.delete("/{user_id}")
def delete_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    db.delete(user)
    db.commit()
    return {"message": "User deleted successfully"}
