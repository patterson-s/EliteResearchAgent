import os
import time
import requests
from pathlib import Path
from mistralai import Mistral
from typing import Dict, Any

class MistralOCR:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("MISTRAL_API_KEY")
        if not self.api_key:
            raise EnvironmentError("MISTRAL_API_KEY not found in environment")
        self.client = Mistral(api_key=self.api_key)
    
    def download_pdf(self, url: str, output_path: Path) -> bool:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            return True
        except Exception as e:
            print(f"  Failed to download: {e}")
            return False
    
    def upload_document(self, file_path: Path) -> str:
        with open(file_path, "rb") as file:
            uploaded_file = self.client.files.upload(
                file={
                    "file_name": file_path.name,
                    "content": file,
                },
                purpose="ocr"
            )
        return uploaded_file.id
    
    def get_signed_url(self, file_id: str) -> str:
        signed_url = self.client.files.get_signed_url(file_id=file_id)
        return signed_url.url
    
    def process_document(self, document_url: str) -> Dict[Any, Any]:
        ocr_response = self.client.ocr.process(
            model="mistral-ocr-latest",
            document={
                "type": "document_url",
                "document_url": document_url,
            }
        )
        return ocr_response.model_dump()
    
    def extract_markdown(self, ocr_data: Dict[Any, Any]) -> str:
        markdown_text = ""
        for page in ocr_data.get("pages", []):
            page_number = page.get("index", "unknown")
            markdown_content = page.get("markdown", "")
            
            markdown_text += f"## Page {page_number}\n\n"
            markdown_text += markdown_content
            markdown_text += "\n\n---\n\n"
        
        return markdown_text
    
    def ocr_pdf_from_url(self, url: str, temp_dir: Path) -> str:
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        pdf_filename = url.split('/')[-1]
        if not pdf_filename.endswith('.pdf'):
            pdf_filename = 'document.pdf'
        
        temp_pdf = temp_dir / pdf_filename
        
        print(f"  Downloading PDF...")
        if not self.download_pdf(url, temp_pdf):
            raise Exception("Failed to download PDF")
        
        print(f"  Uploading to Mistral...")
        file_id = self.upload_document(temp_pdf)
        
        print(f"  Getting signed URL...")
        signed_url = self.get_signed_url(file_id)
        
        print(f"  Processing with OCR...")
        ocr_data = self.process_document(signed_url)
        
        print(f"  Extracting text...")
        markdown_text = self.extract_markdown(ocr_data)
        
        temp_pdf.unlink()
        
        page_count = len(ocr_data.get("pages", []))
        print(f"  Processed {page_count} pages")
        
        return markdown_text