from pathlib import Path
import mimetypes
import re
import aiofiles
import httpx
from starlette.staticfiles import StaticFiles
from starlette.responses import FileResponse, Response
from starlette.exceptions import HTTPException
import os

def _normalize_domain(name: str) -> str:
    """Normalize domain name for Clearbit API"""
    n = name.strip()
    n = re.sub(r"^https?://", "", n, flags=re.IGNORECASE)
    n = n.split("/")[0]
    if n.startswith("www."):
        n = n[4:]
    return n.lower()

async def _save_bytes_atomic(dest: Path, data: bytes) -> None:
    """Atomically save bytes to a file"""
    tmp = dest.with_suffix(dest.suffix + ".part") if dest.suffix else dest.with_name(dest.name + ".part")
    print(f"[save] writing to temporary file: {tmp}")
    
    # Ensure parent directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    async with aiofiles.open(tmp, "wb") as f:
        await f.write(data)
    print(f"[save] replacing {tmp} -> {dest}")
    tmp.replace(dest)
    print(f"[save] saved {dest} (size={dest.stat().st_size} bytes)")

class DynamicStaticFiles:
    """
    Custom ASGI application for dynamic static file serving that:
      - Serves exact file if present
      - Falls back to case-insensitive filename/stem match
      - If still missing, downloads from Clearbit using '{stem}.com', saves and serves it
      - If Clearbit fails, serves 'hiring.png' if present
    """

    def __init__(self, directory: str):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        print(f"[init] DynamicStaticFiles serving from: {self.directory}")

    async def __call__(self, scope, receive, send):
        """ASGI application entry point"""
        if scope["type"] != "http":
            # Not an HTTP request
            response = Response("Not Found", status_code=404)
            await response(scope, receive, send)
            return

        path = scope["path"]
        method = scope["method"]
        
        if method != "GET":
            response = Response("Method Not Allowed", status_code=405)
            await response(scope, receive, send)
            return

        # Remove leading slash and get filename
        filename = path.lstrip("/")
        if not filename:
            response = Response("Not Found", status_code=404)
            await response(scope, receive, send)
            return

        print(f"===== DynamicStaticFiles request: {filename} =====")
        
        try:
            file_path = await self.find_or_create_file(filename)
            if file_path and file_path.exists():
                print(f"[served] file: {file_path}")
                response = FileResponse(str(file_path))
                await response(scope, receive, send)
                return
        except Exception as e:
            print(f"[error] Exception in find_or_create_file: {e}")

        # If we get here, file not found
        print(f"[404] File not found: {filename}")
        response = Response("Not Found", status_code=404)
        await response(scope, receive, send)

    async def find_or_create_file(self, filename: str) -> Path:
        """Find existing file or create it by downloading from Clearbit"""
        
        # 1) Check for exact filename
        exact_path = self.directory / filename
        if exact_path.exists():
            print(f"[found] exact file: {exact_path}")
            return exact_path

        # Extract components
        requested_name = Path(filename).name
        requested_stem = Path(requested_name).stem
        requested_ext = Path(requested_name).suffix.lower()
        
        # Normalize the stem for domain lookup
        normalized_stem = _normalize_domain(requested_stem)
        
        print(f"[info] requested_name='{requested_name}', stem='{requested_stem}', ext='{requested_ext}', normalized='{normalized_stem}'")

        # 2) Case-insensitive scan for existing files
        print(f"[step 2] scanning directory: {self.directory}")
        try:
            files = list(self.directory.iterdir())
            existing_files = [f.name for f in files if f.is_file()]
            print(f"[step 2] existing files: {existing_files}")
            
            for file_path in files:
                if not file_path.is_file():
                    continue
                
                # Check for exact case-insensitive filename match
                if file_path.name.lower() == requested_name.lower():
                    print(f"[found] case-insensitive filename match: {file_path.name}")
                    return file_path
                
                # Check for stem match with any image extension
                file_stem_lower = file_path.stem.lower()
                file_ext_lower = file_path.suffix.lower()
                
                if (file_stem_lower == normalized_stem and 
                    file_ext_lower in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.svg']):
                    print(f"[found] case-insensitive stem match: {file_path.name}")
                    return file_path
                    
        except Exception as e:
            print(f"[step 2] error scanning directory: {e}")

        # 3) Download from Clearbit
        clearbit_domain = f"{normalized_stem}.com"
        clearbit_url = f"https://logo.clearbit.com/{clearbit_domain}"
        print(f"[step 3] attempting to fetch from Clearbit: {clearbit_url}")
        
        try:
            async with httpx.AsyncClient(
                follow_redirects=True, 
                timeout=15.0
            ) as client:
                response = await client.get(
                    clearbit_url,
                    headers={"User-Agent": "DynamicStaticFiles/1.0"}
                )
            
            print(f"[step 3] clearbit response status: {response.status_code}")
            
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "").split(";")[0].strip().lower()
                print(f"[step 3] clearbit content-type: {content_type}")
                
                if content_type.startswith("image/") and len(response.content) > 0:
                    # Determine file extension
                    ext = mimetypes.guess_extension(content_type) or ".png"
                    if ext == ".jpe":
                        ext = ".jpg"
                    
                    # Use the requested extension if it's an image extension
                    if requested_ext in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp']:
                        ext = requested_ext
                    
                    final_name = f"{normalized_stem}{ext}"
                    final_path = self.directory / final_name
                    
                    print(f"[step 3] saving as: {final_name} (size: {len(response.content)} bytes)")
                    
                    # Save the file
                    await _save_bytes_atomic(final_path, response.content)
                    
                    print(f"[success] downloaded and saved: {final_name}")
                    return final_path
                else:
                    print(f"[step 3] invalid content type or empty content: {content_type}")
            else:
                print(f"[step 3] clearbit returned status {response.status_code}")
                
        except httpx.TimeoutException:
            print("[step 3] timeout while fetching from clearbit")
        except Exception as e:
            print(f"[step 3] exception while fetching from clearbit: {e}")

        # 4) Fallback to hiring.png
        hiring_path = self.directory / "hiring.png"
        if hiring_path.exists():
            print("[fallback] serving hiring.png as fallback")
            return hiring_path

        # Nothing found
        print("[final] no file found or created")
        return None