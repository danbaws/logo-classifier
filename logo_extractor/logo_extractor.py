import os
import sys
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import concurrent.futures
import time
from PIL import Image
from io import BytesIO
import re

def get_domain(url):
    """Extract the base domain from a URL."""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return domain

def find_logo_urls(url):
    """Find potential logo URLs from a website."""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Failed to fetch {url}: Status code {response.status_code}")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        logo_urls = []
        
        # Method 1: Look for images with 'logo' in their attributes
        for img in soup.find_all('img'):
            img_src = img.get('src', '')
            img_alt = img.get('alt', '').lower()
            img_class = ' '.join(img.get('class', [])).lower()
            img_id = img.get('id', '').lower()
            
            if any(logo_term in attr for logo_term in ['logo', 'brand', 'header-image'] 
                   for attr in [img_src.lower(), img_alt, img_class, img_id]):
                img_url = urljoin(url, img_src)
                logo_urls.append(img_url)
        
        # Method 2: Look for link tags with rel="icon" or similar
        for link in soup.find_all('link'):
            rel = link.get('rel', [])
            if isinstance(rel, list):
                rel = ' '.join(rel).lower()
            else:
                rel = rel.lower()
                
            if any(icon_term in rel for icon_term in ['icon', 'shortcut icon', 'apple-touch-icon']):
                href = link.get('href', '')
                if href:
                    icon_url = urljoin(url, href)
                    logo_urls.append(icon_url)
        
        # Method 3: Look inside SVGs that might contain logos
        for svg in soup.find_all('svg'):
            svg_class = ' '.join(svg.get('class', [])).lower()
            svg_id = svg.get('id', '').lower()
            
            if any(logo_term in attr for logo_term in ['logo', 'brand'] 
                   for attr in [svg_class, svg_id]):
                # Convert SVG to a data URL
                svg_str = str(svg)
                logo_urls.append(f"data:image/svg+xml;base64,{svg_str}")
        
        return logo_urls
    
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return []

def is_likely_logo(image_content, min_size=16, max_size=500):
    """Check if an image is likely to be a logo based on its properties."""
    try:
        img = Image.open(BytesIO(image_content))
        width, height = img.size
        
        # Check size constraints (logos are usually not too small or too large)
        if width < min_size or height < min_size:
            return False
        if width > max_size or height > max_size:
            return False
        
        # Check if it's not a common web icon size (16x16, 32x32)
        if (width == 16 and height == 16) or (width == 32 and height == 32):
            # Only accept these sizes for favicon
            return True
        
        # Aspect ratio check (logos are usually not extremely wide or tall)
        aspect_ratio = max(width, height) / min(width, height)
        if aspect_ratio > 4:  # Arbitrary threshold
            return False
        
        return True
    except Exception:
        return False

def download_logo(url, domain, output_folder):
    """Download a logo from a URL and save it to the output folder."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Handle data URLs (embedded SVGs)
        if url.startswith('data:image/svg+xml;base64,'):
            svg_content = url.split('base64,')[1]
            file_path = os.path.join(output_folder, f"{domain} - logo.svg")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(svg_content)
            return file_path
        
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None
        
        # Check if it's likely to be a logo
        if not is_likely_logo(response.content):
            return None
        
        # Determine file extension
        content_type = response.headers.get('content-type', '').lower()
        if 'png' in content_type:
            ext = '.png'
        elif 'jpg' in content_type or 'jpeg' in content_type:
            ext = '.jpg'
        elif 'svg' in content_type:
            ext = '.svg'
        elif 'ico' in content_type:
            ext = '.ico'
        else:
            # Try to determine from URL
            url_path = urlparse(url).path.lower()
            if url_path.endswith('.png'):
                ext = '.png'
            elif url_path.endswith('.jpg') or url_path.endswith('.jpeg'):
                ext = '.jpg'
            elif url_path.endswith('.svg'):
                ext = '.svg'
            elif url_path.endswith('.ico'):
                ext = '.ico'
            else:
                # Default to PNG if we can't determine
                ext = '.png'
        
        file_path = os.path.join(output_folder, f"{domain} - logo{ext}")
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        return file_path
    
    except Exception as e:
        print(f"Error downloading logo from {url}: {str(e)}")
        return None

def process_website(website, output_folder):
    """Process a single website to extract its logo."""
    domain = get_domain(website)
    print(f"Processing {domain}...")
    
    logo_urls = find_logo_urls(website)
    if not logo_urls:
        print(f"No potential logos found for {domain}")
        return
    
    success = False
    for url in logo_urls:
        file_path = download_logo(url, domain, output_folder)
        if file_path:
            print(f"Logo downloaded for {domain}: {file_path}")
            success = True
            break  # Stop after finding the first valid logo
    
    if not success:
        print(f"Failed to download any valid logos for {domain}")

def extract_logos_from_parquet(parquet_file, output_folder, column_name='website', max_workers=5):
    """Extract logos from websites listed in a parquet file."""
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Read the parquet file
    try:
        df = pd.read_parquet(parquet_file)
    except Exception as e:
        print(f"Error reading parquet file: {str(e)}")
        return
    
    if column_name not in df.columns:
        print(f"Column '{column_name}' not found in parquet file. Available columns: {df.columns.tolist()}")
        return
    
    # Get list of websites
    websites = df[column_name].dropna().unique().tolist()
    print(f"Found {len(websites)} websites in the parquet file")
    
    # Process websites in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_website, website, output_folder) for website in websites]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in thread: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python logo_extractor.py <parquet_file> [column_name] [output_folder]")
        sys.exit(1)
    
    parquet_file = sys.argv[1]
    column_name = sys.argv[2] if len(sys.argv) > 2 else 'domain'
    output_folder = sys.argv[3] if len(sys.argv) > 3 else 'logo'
    
    print(f"Starting logo extraction from {parquet_file}...")
    print(f"Output folder: {output_folder}")
    print(f"Column name: {column_name}")
    
    extract_logos_from_parquet(parquet_file, output_folder, column_name)
    
    print("Logo extraction complete!")