import os
import json
import logging
import subprocess
import uuid
import tempfile
from pathlib import Path
from datetime import datetime
from base64 import b64encode
import pandas as pd
import pdfplumber
from PIL import Image
import requests
from dotenv import load_dotenv
load_dotenv()
from celery import shared_task
from text_extraction.mongodb_state_db import get_file_document
from data_ingestion.worker import app, process_file

# === Constants ===
VISION_URL = os.getenv("VLM_URL")
TEXT_URL = os.getenv("TEXT_URL")
SOFFICE_PATH = "/usr/bin/soffice"
IMAGE_FORMATS = {"png", "jpg", "jpeg", "bmp", "webp"}


class UniversalDocumentExtractor:
    def __init__(self, debug_mode: bool, enable_vlm: bool, vlm_url: str, vlm_model: str, vlm_prompt: str):
        self.debug_mode = debug_mode
        self.enable_vlm = enable_vlm
        self.vlm_url = vlm_url
        self.vlm_model = vlm_model
        self.vlm_prompt = vlm_prompt

        logging.basicConfig(
            level=logging.DEBUG if debug_mode else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _call_llm(self, prompt: str) -> str:
        response = requests.post(TEXT_URL, json={
            "model": "meta/llama-3.1-70b-instruct",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1024
        }, headers={"Content-Type": "application/json"})
        return response.json()["choices"][0]["message"]["content"]

    def _call_vision(self, image_path: str) -> str:
        prompt = (
            "At the beginning of your response, write: 'Heading/Content: <main subject>' — "
            "Then extract every visible character and detail. Don't skip anything from top bottom of the page to end of the page. "
            "Important note: don’t miss any contact details if present. "
            "If any image or diagram is present, describe it briefly. "
            "Preserve structure and order. Text-only content should be extracted word-for-word."
        )
        with open(image_path, "rb") as img_file:
            img_data = img_file.read()
        b64_string = b64encode(img_data).decode()

        response = requests.post(VISION_URL, json={
            "model": "meta/llama-3.2-11b-vision-instruct",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_string}"}}
            ]}],
            "max_tokens": 2048
        }, headers={"Content-Type": "application/json"})
        return response.json()["choices"][0]["message"]["content"]

    def _convert_to_pdf(self, input_path: Path) -> Path:
        tmpdir = tempfile.mkdtemp()
        subprocess.run([SOFFICE_PATH, "--headless", "--convert-to", "pdf", str(input_path), "--outdir", tmpdir], check=True)
        return next(Path(tmpdir).glob("*.pdf"))

    def _convert_pdf_to_images(self, pdf_path: Path, output_dir: Path) -> list:
        output_dir.mkdir(parents=True, exist_ok=True)
        images = []
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                img = page.to_image(resolution=150).original
                img_path = output_dir / f"page_{i+1}.png"
                img.save(img_path)
                images.append((i + 1, img_path))
        return images

    def _extract_excel(self, file_path: Path) -> dict:
        sheets = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
        extracted = {}
        for name, df in sheets.items():
            df.fillna("", inplace=True)
            csv_text = df.to_csv(index=False)
            prompt = f"At the beginning of your response, write: 'Heading/Content: <main subject>' —\n\nExtract structured content from this Excel sheet '{name}':\n{csv_text}"
            result = self._call_llm(prompt)
            extracted[f"sheet_{name}"] = result
        return extracted

    def _extract_text_file(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def extract_text_from_file(self, file_path: Path) -> dict:
        ext = file_path.suffix.lower()
        extracted_text = {}

        try:
            if ext == ".xlsx":
                extracted_text = self._extract_excel(file_path)

            elif ext.lstrip(".") in IMAGE_FORMATS:
                result = self._call_vision(str(file_path))
                extracted_text["page_1"] = result

            elif ext in {".pdf", ".pptx", ".docx"}:
                if ext in {".pptx", ".docx"}:
                    file_path = self._convert_to_pdf(file_path)

                image_dir = Path("converted_pages")
                pages = self._convert_pdf_to_images(file_path, image_dir)

                for page_num, img_path in pages:
                    page_result = self._call_vision(str(img_path))
                    extracted_text[f"page_{page_num}"] = page_result

            elif ext in {".txt", ".md"}:
                content = self._extract_text_file(file_path)
                extracted_text["page_1"] = content

            else:
                extracted_text["error"] = "Unsupported file format"

        except Exception as e:
            self.logger.error(f"Extraction failed for {file_path}: {str(e)}")
            extracted_text["error"] = str(e)

        return extracted_text


def _update_extraction_json(output_path: Path, new_record: dict):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    existing_data = []

    if output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except json.JSONDecodeError:
            pass

    for i, record in enumerate(existing_data):
        if record.get("file_path") == new_record["file_path"]:
            existing_data[i] = new_record
            break
    else:
        existing_data.append(new_record)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)


@shared_task(bind=True, name="tasks.docvlm_extraction_task", acks_late=True, max_retries=2, time_limit=3600)
def docvlm_extraction_task(self, user_id: str, filepath_str: str, file_hash: str):
    filepath = Path(filepath_str)
    filename = filepath.name

    try:
        print(f"🚀 ({user_id}) Starting extraction for: {filename}")
        db_record = get_file_document(user_id, filepath_str)
        if not db_record:
            raise Exception(f"Could not find database record for {filename}")

        extractor = UniversalDocumentExtractor(
            debug_mode=True,
            enable_vlm=True,
            vlm_url=VISION_URL,
            vlm_model="meta/llama-3.2-11b-vision-instruct",
            vlm_prompt="Extract ALL content page by page"
        )

        extracted_text = extractor.extract_text_from_file(filepath)

        final_record = {
            "uuid": db_record.get("uuid"),
            "sha256": db_record.get("sha256"),
            "user_id": user_id,
            "file_name": filename,
            "file_path": filepath_str,
            "folder_path": str(filepath.parent),
            "status": db_record.get("status"),
            "last_modified": db_record.get("last_modified"),
            "extracted_text": extracted_text
        }

        output_path = Path("extraction_results") / f"{user_id}.json"
        _update_extraction_json(output_path, final_record)

        print(f"✅ ({user_id}) Successfully processed and saved metadata for {filename}.")
        print(f"   -> Results saved to: {output_path}")

        process_file.apply_async(args=[final_record])

    except Exception as e:
        print(f"❌ ({user_id}) Error processing {filename}: {e}")
        raise self.retry(exc=e, countdown=60)