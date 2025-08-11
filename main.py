import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from db import engine, Base
from api import job_router, user_router
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from gradio_interface import create_interface
import gradio as gr

from upload_image import get_company_image

# Initialize FastAPI app
app = FastAPI()

# Allow all CORS origins (Temporary for testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change this to specific domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create database tables
Base.metadata.create_all(bind=engine)

# Include Routers
app.include_router(job_router.router)
app.include_router(user_router.router)

#Greetings
@app.get("")
async def root():
    return {"message": "Hello World"}

#CMS System Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploaded_images")
app.mount("/images", StaticFiles(directory=UPLOAD_DIR), name="images")

@app.post("/upload-image/")
async def upload_image(company_name: str):
    image_filename = get_company_image(company_name)
    return {"message": "File uploaded", "url": f"{image_filename}"}

@app.get("/cms/Admin", response_class=HTMLResponse)
async def read_index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)

gradio_blocks = create_interface()
app = gr.mount_gradio_app(app, gradio_blocks, path="/add-job/Admin")
    
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 4000)),
        reload=True,
    )