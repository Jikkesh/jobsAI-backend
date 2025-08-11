import gradio as gr
import os
import requests
from PIL import Image
from io import BytesIO
from datetime import datetime
from ai_job_helper import generate_ai_enhanced_content
from sqlalchemy.orm import Session
from models import Job
from db import SessionLocal
import time
from const import alias_map

# Constants
JOB_CATEGORIES = ["Fresher", "Internship", "Remote", "Experienced"]
EXPERIENCE_LEVELS = ["Fresher","1-3 years", "3-5 years", "5+ years"]
QUALIFICATIONS = ["Any Degree", "Any Engineering Degree", "B.E/CSE", "B.Tech", "B.Sc"]

class ImageManager:
    def __init__(self):
        self.existing_images = set()
        self.upload_dir = "uploaded_images"
        os.makedirs(self.upload_dir, exist_ok=True)
        self._load_existing_images()
    
    def _load_existing_images(self):
        """Load existing image filenames"""
        if os.path.exists(self.upload_dir):
            for filename in os.listdir(self.upload_dir):
                if filename.lower().endswith('.png'):
                    # Remove .png extension to get company name
                    company_name = filename[:-4]  # Remove last 4 characters (.png)
                    self.existing_images.add(company_name.lower())

    def get_company_image(self, company_name: str, size=(400, 200)) -> str:
        """Fetch company logo from Clearbit API with alias and suffix handling"""
        if not company_name or company_name.lower() == "not specified":
            return ""

        # Clean filename for saving (use full company name)
        clean_company_name = company_name.replace('/', '_').replace('\\', '_')
        image_filename = f"{clean_company_name}.png"
        save_path = os.path.join("uploaded_images", image_filename)

        # Step 1: Check if image already exists in the pool
        if os.path.exists(save_path):
            print(f"  🖼️ Company image already exists in pool: {image_filename}")
            self.existing_images.add(company_name.lower())
            return save_path  # Return full path instead of just filename

        # Step 2: If not found, prepare for API fetch
        # Normalize name for API lookup
        company_key = company_name.strip().lower()
        
        # Step 3: Handle known aliases or suffix trimming for API lookup
        if company_key in alias_map:
            lookup_name = alias_map[company_key]
        else:
            # Remove common suffixes like 'technologies', 'technology' for API lookup
            lookup_name = company_key
            for suffix in [" technologies", " technology", " solutions", " solution", " pvt ltd", " pvt. ltd", " private limited", " ltd", " limited", " inc", " corporation", " corp"]:
                if lookup_name.endswith(suffix):
                    lookup_name = lookup_name.replace(suffix, "")
                    break
            lookup_name = lookup_name.strip()

        # Build domain for logo fetching
        domain = lookup_name.replace(" ", "").replace("pvt", "").replace("ltd", "") + ".com"
        logo_url = f"https://logo.clearbit.com/{domain}"

        try:
            print(f"  🌐 Fetching logo for {company_name} using domain: {domain}...")
            response = requests.get(logo_url, timeout=10)
            response.raise_for_status()

            logo = Image.open(BytesIO(response.content)).convert("RGBA")
            white_bg = Image.new("RGBA", logo.size, (255, 255, 255, 255))
            combined = Image.alpha_composite(white_bg, logo)

            combined.thumbnail(size, Image.LANCZOS)
            final = Image.new("RGB", size, (255, 255, 255))
            position = (
                (size[0] - combined.width) // 2,
                (size[1] - combined.height) // 2
            )
            final.paste(combined.convert("RGB"), position)

            # Save with full company name to uploaded_images
            final.save(save_path)
            self.existing_images.add(company_name.lower())
            print(f"  ✅ Successfully saved logo to: {save_path}")
            return save_path    # Return full path instead of just filename

        except requests.RequestException as e:
            print(f"  ❌ Failed to fetch logo for {company_name}: {e}")
            return ""
        except Exception as e:
            print(f"  ❌ Error processing logo for {company_name}: {e}")
            return ""

    def save_uploaded_image(self, image_file, company_name: str) -> str:
        """Save uploaded image to the upload directory"""
        if not image_file or not company_name:
            return ""
        
        # Use full company name with .png extension
        clean_company_name = company_name.replace('/', '_').replace('\\', '_')
        image_filename = f"{clean_company_name}.png"
        save_path = os.path.join(self.upload_dir, image_filename)
        
        # Check if image already exists in the pool
        if os.path.exists(save_path):
            print(f"  🖼️ Company image already exists in pool, skipping upload: {image_filename}")
            self.existing_images.add(company_name.lower())
            return save_path
        
        try:
            # Handle different input types (file path or file object)
            if hasattr(image_file, 'name'):
                # It's a file object from Gradio
                image = Image.open(image_file.name)
            else:
                # It's a file path string
                image = Image.open(image_file)
            
            # Convert to RGB to avoid RGBA issues
            if image.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', image.size, (255, 255, 255))
                if image.mode == 'P':
                    image = image.convert('RGBA')
                background.paste(image, mask=image.split()[-1] if image.mode == 'RGBA' else None)
                image = background
            elif image.mode != 'RGB':
                image = image.convert("RGB")
            
            # Resize to standard size
            image.thumbnail((400, 200), Image.LANCZOS)
            final = Image.new("RGB", (400, 200), (255, 255, 255))
            position = (
                (400 - image.width) // 2,
                (200 - image.height) // 2
            )
            final.paste(image, position)
            
            final.save(save_path, "PNG")
            self.existing_images.add(company_name.lower())
            print(f"  ✅ Successfully saved uploaded image: {image_filename}")
            return save_path
            
        except Exception as e:
            print(f"  ❌ Error saving uploaded image: {e}")
            return ""

# Initialize image manager
image_manager = ImageManager()

def fetch_company_image(company_name):
    """Fetch company image when company name is entered"""
    if not company_name or len(str(company_name).strip()) < 2:
        return None, "", gr.update(visible=True), "Enter company name to fetch logo"

    try:
        image_path = image_manager.get_company_image(str(company_name).strip())

        if image_path and os.path.exists(str(image_path)):
            print(f"  ✅ Image found at: {image_path}")
            return image_path, image_path, gr.update(visible=False), f"✅ Logo found for {company_name}"

        return None, "", gr.update(visible=True), f"❌ No logo found for {company_name}. Please upload an image."
    except Exception as e:
        print(f"Error fetching company image: {e}")
        return None, "", gr.update(visible=True), f"❌ Error fetching logo for {company_name}. Please upload an image."
    

def handle_manual_upload(image_file, company_name):
    """Handle manual image upload"""
    if not image_file:
        return None, "Please select an image file"
    
    if not company_name or len(str(company_name).strip()) < 2:
        return None, "Please enter company name first"
    
    try:
        # Save with full company name
        image_path = image_manager.save_uploaded_image(image_file, str(company_name).strip())
        
        if image_path and os.path.exists(str(image_path)):
            return str(image_path), f"✅ Image uploaded successfully for {company_name}"
        else:
            return None, "❌ Failed to upload image"
    except Exception as e:
        print(f"Error handling manual upload: {e}")
        return None, f"❌ Error uploading image: {str(e)}"

def process_job_submission(
    category,
    company_name,
    job_role,
    website_link,
    state,
    city,
    experience,
    batch,
    salary_package,
    current_image_path,
    manual_upload,
    job_desc_preview,
    resp_preview,
    company_preview,
    process_preview,
    qual_preview
):
    print("Image", current_image_path, "Manual Upload", manual_upload)
    """Process the job submission and store in DB"""
    try:
        # Validate required fields
        if not company_name or not str(company_name).strip():
            return "❌ Company name is required"
        
        if not job_role or not str(job_role).strip():
            return "❌ Job role is required"
            
        if not salary_package or not str(salary_package).strip():
            return "❌ Salary package is required"

        # Use the previewed/generated content
        job_description = str(job_desc_preview) if job_desc_preview else ""
        key_responsibility = str(resp_preview) if resp_preview else ""
        about_company = str(company_preview) if company_preview else ""
        selection_process = str(process_preview) if process_preview else ""
        qualification_details = str(qual_preview) if qual_preview else ""
        
    except Exception as e:
        return f"❌ Error processing form data: {str(e)}"

    # Handle image - save only the filename in DB
    image_filename = None
    try:
        if manual_upload and hasattr(manual_upload, 'name'):
            # If manual upload is provided, use it
            image_path = image_manager.save_uploaded_image(manual_upload, str(company_name).strip())
            if image_path and os.path.exists(image_path):
                clean_name = str(company_name).strip().replace('/', '_').replace('\\', '_')
                image_filename = f"{clean_name}.png"
        elif current_image_path and os.path.exists(str(current_image_path)):
            # Use the automatically fetched image
            clean_name = str(company_name).strip().replace('/', '_').replace('\\', '_')
            image_filename = f"{clean_name}.png"
            print(f"  ✅ Using fetched image for DB: {image_filename}")
            print(f"  ✅ Image exists at: {current_image_path}")
    except Exception as e:
        print(f"Warning: Error handling image: {e}")
        # Continue without image

    try:
        db = SessionLocal()
        job_entry = Job(
            category=str(category) if category else "",
            company_name=str(company_name).strip(),
            job_role=str(job_role).strip(),
            website_link=str(website_link) if website_link else "",
            state=str(state) if state else "",
            city=str(city) if city else "",
            experience=str(experience) if experience else "",
            qualification=qualification_details,
            batch=str(batch) if batch else "",
            salary_package=str(salary_package).strip(),
            job_description=job_description,
            key_responsibility=key_responsibility,
            about_company=about_company,
            selection_process=selection_process,
            image=image_filename,
            posted_on=datetime.now(),  # Add the missing posted_on field
        )
        db.add(job_entry)
        db.commit()
        db.refresh(job_entry)
        job_id = job_entry.id
        db.close()
        return f"✅ Job uploaded successfully! Job ID: {job_id}"
    except Exception as e:
        if 'db' in locals():
            db.rollback()
            db.close()
        return f"❌ Error saving job data: {str(e)}"

def generate_and_state(job_details, company_name, job_role):
    """Generate AI previews and update progress bar"""
    if not job_details or len(job_details.strip()) < 50:
        empty = ""
        return ("Please enter more detailed job information.",) + (empty,) * 9

    result = generate_ai_enhanced_content(job_details, company_name, job_role)

    return (
        result["job_description"],
        result["key_responsibility"],
        result["about_company"],
        result["selection_process"],
        result["qualification"],
        # states
        result["job_description"],
        result["key_responsibility"],
        result["about_company"],
        result["selection_process"],
        result["qualification"]
    )

def create_interface():
    """Create and configure the Gradio interface"""
    with gr.Blocks(title="Job Entry System", theme=gr.themes.Soft()) as app:
        gr.Markdown("# 🏢 Job Entry System")
        gr.Markdown("Enter job details below. The system will automatically fetch company logos and generate detailed sections using AI.")

        with gr.Row():
            with gr.Column(scale=1):
                category = gr.Dropdown(choices=JOB_CATEGORIES, label="* Category")
                company_name = gr.Textbox(label="* Company Name", placeholder="Enter company name")
                job_role = gr.Textbox(label="* Job Role", placeholder="Enter job role/title")
                website_link = gr.Textbox(label="Website Link (Optional)", placeholder="https://company.com")
                state = gr.Textbox(label="State", placeholder="Enter state")
                city = gr.Textbox(label="City", placeholder="Enter city")
                
            with gr.Column(scale=1):
                experience = gr.Dropdown(choices=EXPERIENCE_LEVELS, label="Experience")
                batch = gr.Textbox(label="Batch (Optional)", placeholder="e.g., 2022-2023")
                salary_package = gr.Textbox(label="* Salary Package", placeholder="e.g., 5-8 LPA")
                
                # Company Image Section
                with gr.Group():
                    gr.Markdown("### Company Logo")
                    company_image_display = gr.Image(label="Company Logo", height=200, show_label=True)
                    image_status = gr.Markdown("Enter company name to fetch logo automatically")
                    
                    manual_upload = gr.File(
                        label="Upload Custom Image (if logo not found)", 
                        file_types=["image"],
                        visible=True
                    )
                    upload_btn = gr.Button("Upload Custom Image", size="sm")

        # Hidden state for current image path
        current_image_path = gr.State()

        # Auto-fetch image when company name changes
        company_name.change(
            fn=fetch_company_image,
            inputs=[company_name],
            outputs=[company_image_display, current_image_path, manual_upload, image_status]
        )

        # Handle manual upload
        upload_btn.click(
            fn=handle_manual_upload,
            inputs=[manual_upload, company_name],
            outputs=[company_image_display, image_status]
        ).then(
            fn=lambda img: img,
            inputs=[company_image_display],
            outputs=[current_image_path]
        )

        job_details = gr.TextArea(label="Full Job Details", lines=10, placeholder="Paste the complete job description here...")

        # Preview sections
        with gr.Accordion("Preview Generated Content", open=False):
            with gr.Tabs():
                with gr.TabItem("Job Description"):
                    job_desc_preview = gr.Markdown()
                with gr.TabItem("Key Responsibilities"):
                    resp_preview = gr.Markdown()
                with gr.TabItem("About Company"):
                    company_preview = gr.Markdown()
                with gr.TabItem("Selection Process"):
                    process_preview = gr.Markdown()
                with gr.TabItem("Qualification Details"):
                    qual_preview = gr.Markdown()

        # Hidden state holders
        job_desc_state = gr.State()
        resp_state = gr.State()
        company_state = gr.State()
        process_state = gr.State()
        qual_state = gr.State()

        # Preview button
        preview_btn = gr.Button("🤖 Generate Content using AI", variant="secondary")
        preview_btn.click(
            fn=generate_and_state,
            inputs=[job_details, company_name, job_role],
            outputs=[
                job_desc_preview, resp_preview, company_preview,
                process_preview, qual_preview,
                job_desc_state, resp_state, company_state,
                process_state, qual_state
            ],
            show_progress=True
        )

        # Progress and Submit section
        with gr.Group():
            progress_status = gr.Markdown("Ready to submit job")
            submit_btn = gr.Button("🚀 Submit Job", variant="primary", size="lg")

        # Submit functionality
        submit_btn.click(
            fn=process_job_submission,
            inputs=[
                category, company_name, job_role, website_link,
                state, city, experience, batch, salary_package,
                current_image_path, manual_upload,
                job_desc_state, resp_state, company_state,
                process_state, qual_state
            ],
            outputs=progress_status,
            show_progress=True
        )

    return app

if __name__ == "__main__":
    app = create_interface()
    app.launch(debug=True, share=False)