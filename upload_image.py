from PIL import Image
from io import BytesIO
import os
import requests
from pathlib import Path
from const import alias_map

images_dir = Path("uploaded_images")
images_dir.mkdir(exist_ok=True)

def get_company_image(company_name: str, size=(400, 200)) -> str:
    if not company_name or company_name == "Not specified":
        return ""
    
    lookup_name = alias_map.get(company_name.strip().lower(), company_name.strip().lower())
    clean_company_name = company_name.replace('/', '_').replace('\\', '_')
    image_filename = f"{clean_company_name}.png"
    save_path = images_dir / image_filename

    existing_images = {p.stem.lower() for p in images_dir.glob("*.png")}
    if company_name.lower() in existing_images or save_path.exists():
        print(f"  🖼️ Company image already exists: {image_filename}")
        return image_filename

    domain = lookup_name.lower().replace(" ", "").replace("pvt", "").replace("ltd", "").replace("&", "and") + ".com"
    logo_url = f"https://logo.clearbit.com/{domain}"

    try:
        print(f"  🌐 Fetching logo for {lookup_name}...")
        response = requests.get(logo_url, timeout=10)
        response.raise_for_status()

        logo = Image.open(BytesIO(response.content)).convert("RGBA")
        white_bg = Image.new("RGBA", logo.size, (255, 255, 255, 255))
        combined = Image.alpha_composite(white_bg, logo)

        combined.thumbnail(size, Image.Resampling.LANCZOS)
        final = Image.new("RGB", size, (255, 255, 255))
        position = ((size[0] - combined.width) // 2, (size[1] - combined.height) // 2)
        final.paste(combined.convert("RGB"), position)

        final.save(save_path)
        print(f"  ✅ Successfully saved logo: {image_filename}")
        return image_filename

    except requests.RequestException as e:
        print(f"  ❌ Failed to fetch logo for {company_name}: {e} - using default image")
        return 'hiring.png'
    except Exception as e:
        print(f"  ❌ Error processing logo for {company_name}: {e} - using default image")
        return 'hiring.png'
