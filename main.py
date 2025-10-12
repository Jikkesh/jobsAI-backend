import os
import uuid
from pathlib import Path as PathLib  # Import pathlib's Path with alias
from fastapi import FastAPI, File, HTTPException, Path, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from db import engine, Base
from api import job_router, user_router
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from gradio_interface import create_interface
import gradio as gr
from image_processor import DynamicStaticFiles
from upload_image import get_company_image

# Initialize FastAPI app
app = FastAPI()

# Allow all CORS origins (Temporary for testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create database tables
Base.metadata.create_all(bind=engine)

# Include Routers
app.include_router(job_router.router)
app.include_router(user_router.router)

@app.get("")
async def root():
    return {"message": "Hello World"}

# CMS System Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup dynamic image serving
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploaded_images")

app.mount("/images", DynamicStaticFiles(directory=str(UPLOAD_DIR)), name="images")

@app.post("/upload-image/")
async def upload_image(company_name: str):
    image_filename = get_company_image(company_name)
    return {"message": "File uploaded", "url": f"{image_filename}"}

@app.get("/cms/Admin", response_class=HTMLResponse)
async def read_index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)

# Blog image upload configuration
BLOG_UPLOAD_DIR = PathLib("uploaded_images")  # Use PathLib
BASE_URL = "https://api.jobsai.in"
BLOG_UPLOAD_DIR.mkdir(exist_ok=True)

def generate_unique_filename(original_filename: str) -> str:
    """Generate a unique filename to avoid collisions"""
    ext = PathLib(original_filename).suffix.lower()  # Use PathLib
    unique_id = uuid.uuid4().hex[:12]
    return f"{unique_id}{ext}"

@app.post("/upload-blog-image")
async def upload_blog_image(file: UploadFile = File(...)):
    """
    Upload an image file for blog
    
    Returns:
        - image_url: Full URL to access the image
        - image_path: Relative path to the image
        - filename: Saved filename
    """
    # Validate file
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    # Generate unique filename
    unique_filename = generate_unique_filename(file.filename)
    file_path = BLOG_UPLOAD_DIR / unique_filename
    
    # Save file
    contents = await file.read()
    try:
        with open(file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")
    
    # Construct response
    image_url = f"{BASE_URL}/images/{unique_filename}"
    
    return {
        "success": True,
        "message": "Image uploaded successfully",
        "image_url": image_url,
        "image_path": f"/images/{unique_filename}",
        "filename": unique_filename,
        "original_filename": file.filename,
        "size_bytes": len(contents)
    }

gradio_blocks = create_interface()
app = gr.mount_gradio_app(app, gradio_blocks, path="/add-job/Admin")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 4000)),
        reload=True,
    )