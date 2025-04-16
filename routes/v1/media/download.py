from flask import Blueprint, current_app, jsonify
from app_utils import *
import logging
import os
import yt_dlp
import tempfile
import time
import traceback
import shutil
import sys
import json
from werkzeug.utils import secure_filename
import uuid
from services.cloud_storage import upload_file
from services.authentication import authenticate
from services.file_management import download_file
from urllib.parse import quote

v1_media_download_bp = Blueprint('v1_media_download', __name__)
logger = logging.getLogger(__name__)

# Define a global error handler for this blueprint
@v1_media_download_bp.errorhandler(Exception)
def handle_error(error):
    logger.error(f"Unhandled exception in media download: {str(error)}")
    logger.error(traceback.format_exc())
    return jsonify({"error": "Media download failed", "message": str(error)}), 500

@v1_media_download_bp.route('/v1/BETA/media/download', methods=['POST'])
@authenticate
@validate_payload({
    "type": "object",
    "properties": {
        "media_url": {"type": "string", "format": "uri"},
        "webhook_url": {"type": "string", "format": "uri"},
        "id": {"type": "string"},
        "format": {
            "type": "object",
            "properties": {
                "quality": {"type": "string"},
                "format_id": {"type": "string"},
                "resolution": {"type": "string"},
                "video_codec": {"type": "string"},
                "audio_codec": {"type": "string"}
            }
        },
        "audio": {
            "type": "object",
            "properties": {
                "extract": {"type": "boolean"},
                "format": {"type": "string"},
                "quality": {"type": "string"}
            }
        },
        "thumbnails": {
            "type": "object",
            "properties": {
                "download": {"type": "boolean"},
                "download_all": {"type": "boolean"},
                "formats": {"type": "array", "items": {"type": "string"}},
                "convert": {"type": "boolean"},
                "embed_in_audio": {"type": "boolean"}
            }
        },
        "subtitles": {
            "type": "object",
            "properties": {
                "download": {"type": "boolean"},
                "languages": {"type": "array", "items": {"type": "string"}},
                "formats": {"type": "array", "items": {"type": "string"}}
            }
        },
        "download": {
            "type": "object",
            "properties": {
                "max_filesize": {"type": "integer"},
                "rate_limit": {"type": "string"},
                "retries": {"type": "integer"}
            }
        }
    },
    "required": ["media_url"],
    "additionalProperties": False
})
@queue_task_wrapper(bypass_queue=False)
def download_media(job_id, data):
    """
    Completely rewritten function that handles media downloads in a more robust way.
    Uses a simpler approach and better error handling.
    """
    media_url = data['media_url']
    format_options = data.get('format', {})
    audio_options = data.get('audio', {})
    extract_audio = audio_options.get('extract', False)
    
    logger.info(f"Job {job_id}: Received download request for {media_url}")
    
    # Create a more persistent temporary directory
    # We'll manually clean it up to ensure it doesn't disappear during processing
    temp_dir = os.path.join(tempfile.gettempdir(), f"media_download_{job_id}_{uuid.uuid4().hex}")
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        # STEP 1: Determine if we're downloading a video or just audio
        download_audio_only = extract_audio and not format_options
        
        # STEP 2: Configure the simplest possible options for maximum reliability
        ydl_opts = {
            'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
            'quiet': False,
            'no_warnings': False,
            'ignoreerrors': False,
            'verbose': True,
            'writeinfojson': True,
            'paths': {'temp': temp_dir, 'home': temp_dir},
            'retries': 5,
            'fragment_retries': 5,
            'skip_unavailable_fragments': True,
            'keepvideo': extract_audio,  # Keep video if extracting audio
        }
        
        # STEP 3: Set the format based on what we're doing
        if download_audio_only:
            logger.info("Downloading audio only")
            ydl_opts['format'] = 'bestaudio/best'
            audio_format = audio_options.get('format', 'mp3')
            audio_quality = audio_options.get('quality', '192')
            
            # Add audio extraction
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': audio_format,
                'preferredquality': audio_quality,
            }]
        elif format_options and format_options.get('quality'):
            logger.info(f"Using specified quality: {format_options.get('quality')}")
            ydl_opts['format'] = format_options.get('quality')
            
            # Important: We still need to set merge format for combined formats
            if '+' in format_options.get('quality'):
                logger.info("Format contains a merge specification, setting merge_output_format")
                ydl_opts['merge_output_format'] = 'mp4'
                
            # Handle both video download and audio extraction
            if extract_audio:
                logger.info("Adding audio extraction processor alongside video download")
                audio_format = audio_options.get('format', 'mp3')
                audio_quality = audio_options.get('quality', '192')
                
                # Need to ensure we're keeping the video
                ydl_opts['keepvideo'] = True
                
                # Add audio extraction postprocessor
                if 'postprocessors' not in ydl_opts:
                    ydl_opts['postprocessors'] = []
                    
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': audio_format,
                    'preferredquality': audio_quality,
                    'nopostoverwrites': False
                })
        else:
            logger.info("Using best video+audio format")
            ydl_opts['format'] = 'bestvideo+bestaudio/best'
            ydl_opts['merge_output_format'] = 'mp4'
            
            # Handle audio extraction if requested
            if extract_audio:
                logger.info("Adding audio extraction with default video")
                audio_format = audio_options.get('format', 'mp3')
                audio_quality = audio_options.get('quality', '192')
                
                # Need to ensure we're keeping the video
                ydl_opts['keepvideo'] = True
                
                # Add audio extraction postprocessor
                if 'postprocessors' not in ydl_opts:
                    ydl_opts['postprocessors'] = []
                    
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': audio_format,
                    'preferredquality': audio_quality,
                    'nopostoverwrites': False
                })
        
        # STEP 4: Create hooks to monitor the download and postprocessing
        downloaded_files = []
        
        def download_hook(d):
            if d['status'] == 'finished':
                logger.info(f"Download finished: {d['filename']}")
                downloaded_files.append(d['filename'])
                
        def postprocess_hook(d):
            if d.get('status') == 'finished':
                logger.info(f"Postprocessing finished: {d.get('info_dict', {}).get('filepath')}")
                if 'destination' in d and d['destination']:
                    logger.info(f"Postprocessed file destination: {d['destination']}")
                    downloaded_files.append(d['destination'])
                elif 'filepath' in d and d['filepath']:
                    logger.info(f"Postprocessed file path: {d['filepath']}")
                    downloaded_files.append(d['filepath'])
                    
        ydl_opts['progress_hooks'] = [download_hook]
        ydl_opts['postprocessor_hooks'] = [postprocess_hook]
        
        # STEP 5: First attempt - standard download with yt-dlp
        logger.info("Starting download with yt-dlp...")
        info = None
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(media_url, download=True)
                
            if not info:
                raise ValueError("No information returned from yt-dlp")
                
            logger.info(f"Downloaded files via hook: {downloaded_files}")
            
            if not downloaded_files:
                # Check if files exist despite hook not catching them
                files = os.listdir(temp_dir)
                media_files = [f for f in files if not f.endswith('.info.json') and not f.endswith('.description')]
                
                if media_files:
                    logger.info(f"Files in directory: {media_files}")
                    downloaded_files = [os.path.join(temp_dir, f) for f in media_files]
                else:
                    raise FileNotFoundError("No files downloaded")
                    
        except Exception as e:
            logger.error(f"First download attempt failed: {str(e)}")
            
            # STEP 6: Fallback - simplified download with minimal options
            try:
                logger.info("Trying fallback download...")
                fallback_opts = {
                    'format': 'best',
                    'outtmpl': os.path.join(temp_dir, 'fallback.%(ext)s'),
                    'quiet': False,
                    'verbose': True,
                    'no_warnings': False,
                    'ignoreerrors': False,
                    'noplaylist': True,
                    'writeinfojson': True,
                }
                
                if extract_audio:
                    fallback_opts['postprocessors'] = [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': audio_options.get('format', 'mp3'),
                        'preferredquality': audio_options.get('quality', '192'),
                    }]
                
                with yt_dlp.YoutubeDL(fallback_opts) as ydl:
                    info = ydl.extract_info(media_url, download=True)
                
                # Check for downloaded files
                files = os.listdir(temp_dir)
                fallback_files = [f for f in files if f.startswith('fallback.') and not f.endswith('.info.json')]
                
                if fallback_files:
                    logger.info(f"Fallback files: {fallback_files}")
                    downloaded_files = [os.path.join(temp_dir, f) for f in fallback_files]
                else:
                    # Look for any other media files
                    media_files = [f for f in files if not f.endswith('.info.json') and not f.endswith('.description')]
                    
                    if media_files:
                        logger.info(f"Found other media files: {media_files}")
                        downloaded_files = [os.path.join(temp_dir, f) for f in media_files]
                    else:
                        raise FileNotFoundError("No files downloaded in fallback attempt")
                
            except Exception as fallback_error:
                logger.error(f"Fallback download failed: {str(fallback_error)}")
                
                # STEP 7: Final attempt - direct download with explicit filename
                try:
                    logger.info("Trying direct download via subprocess...")
                    import subprocess
                    final_output = os.path.join(temp_dir, "direct_download.mp4")
                    
                    cmd = [
                        'yt-dlp',
                        '--format', 'best',
                        '--output', final_output,
                        '--no-playlist',
                        media_url
                    ]
                    
                    logger.info(f"Executing command: {' '.join(cmd)}")
                    subprocess.run(cmd, check=True)
                    
                    if os.path.exists(final_output) and os.path.getsize(final_output) > 0:
                        logger.info(f"Direct download successful: {final_output}")
                        downloaded_files = [final_output]
                        
                        # Create minimal info
                        if not info:
                            info = {
                                'id': 'direct_download',
                                'title': os.path.basename(media_url),
                                'ext': 'mp4',
                                'format_id': 'best'
                            }
                    else:
                        raise FileNotFoundError(f"File not found or zero size after direct download")
                        
                except Exception as direct_error:
                    logger.error(f"Direct download failed: {str(direct_error)}")
                    raise RuntimeError(f"All download attempts failed")
        
        # STEP 8: Process files for upload
        if not downloaded_files:
            raise FileNotFoundError("No files were downloaded")
            
        logger.info(f"Processing {len(downloaded_files)} downloaded files")
        
        # STEP 9: Handle video and audio (if requested)
        video_file = None
        audio_file = None
        
        # Find the main file (video or audio-only)
        for filepath in downloaded_files:
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                file_ext = os.path.splitext(filepath)[1].lower()
                
                # Check for video files first
                if file_ext in ['.mp4', '.webm', '.mkv', '.mov', '.avi', '.flv']:
                    if not video_file:
                        video_file = filepath
                        logger.info(f"Found video file: {video_file}")
                        
                # Then look for audio files from extraction
                elif file_ext in ['.mp3', '.m4a', '.wav', '.aac', '.opus', '.flac']:
                    if not audio_file and (file_ext == f'.{audio_options.get("format", "mp3")}' or not extract_audio):
                        # Prioritize the requested format
                        audio_file = filepath
                        logger.info(f"Found audio file: {audio_file}")
                    elif not audio_file:
                        # Fallback to any audio file
                        audio_file = filepath
                        logger.info(f"Found fallback audio file: {audio_file}")
                        
        # Special case: if we have a .m4a file but requested mp3 and no mp3 was found
        if extract_audio and audio_options.get('format', 'mp3') == 'mp3' and not audio_file:
            for filepath in downloaded_files:
                if os.path.exists(filepath) and filepath.endswith('.m4a'):
                    logger.info(f"Found .m4a file but mp3 was requested. Converting manually.")
                    try:
                        # Try to convert it manually
                        import subprocess
                        m4a_file = filepath
                        mp3_file = os.path.splitext(filepath)[0] + '.mp3'
                        
                        cmd = [
                            'ffmpeg',
                            '-i', m4a_file,
                            '-codec:a', 'libmp3lame',
                            '-q:a', audio_options.get('quality', '192').replace('k', ''),
                            mp3_file
                        ]
                        
                        logger.info(f"Running FFmpeg command: {' '.join(cmd)}")
                        subprocess.run(cmd, check=True)
                        
                        if os.path.exists(mp3_file) and os.path.getsize(mp3_file) > 0:
                            audio_file = mp3_file
                            logger.info(f"Manual conversion successful: {audio_file}")
                            
                    except Exception as conv_error:
                        logger.error(f"Manual conversion failed: {str(conv_error)}")
            
        # If we didn't find specific file types, just take the first non-empty file
        if not video_file and not audio_file and downloaded_files:
            for filepath in downloaded_files:
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    if extract_audio:
                        audio_file = filepath
                    else:
                        video_file = filepath
                    break
        
        # Check that we have at least one file
        if not video_file and not audio_file:
            raise FileNotFoundError("Could not find any valid downloaded files")
        
        # STEP 10: Upload files
        cloud_urls = {}
        
        try:
            # Upload video file if available
            if video_file:
                logger.info(f"Uploading video file: {video_file} ({os.path.getsize(video_file)} bytes)")
                try:
                    video_cloud_url = upload_file(video_file)
                    cloud_urls['video'] = video_cloud_url
                    logger.info(f"Video upload successful: {video_cloud_url}")
                except Exception as e:
                    logger.error(f"Video upload failed: {str(e)}")
                    # This is critical for video-only downloads
                    if not extract_audio:
                        raise
            
            # Upload audio file if available
            if audio_file:
                logger.info(f"Uploading audio file: {audio_file} ({os.path.getsize(audio_file)} bytes)")
                try:
                    audio_cloud_url = upload_file(audio_file)
                    cloud_urls['audio'] = audio_cloud_url
                    logger.info(f"Audio upload successful: {audio_cloud_url}")
                except Exception as e:
                    logger.error(f"Audio upload failed: {str(e)}")
                    # This is critical for audio-only downloads
                    if extract_audio and not video_file:
                        raise
        
        except Exception as upload_error:
            logger.error(f"Upload process failed: {str(upload_error)}")
            raise RuntimeError(f"Failed to upload media: {str(upload_error)}")
        
        # STEP 11: Build response
        if not info:
            # Create minimal info if we don't have it
            info = {
                'id': 'unknown',
                'title': os.path.basename(media_url),
                'ext': os.path.splitext(video_file or audio_file)[1].lstrip('.') if (video_file or audio_file) else 'mp4',
                'format_id': 'unknown'
            }
        
        response = {"media": {}}
        
        # Add video info if present
        if 'video' in cloud_urls:
            response["media"] = {
                "media_url": cloud_urls['video'],
                "title": info.get('title', 'Unknown Title'),
                "format_id": info.get('format_id', 'unknown'),
                "ext": info.get('ext', 'mp4'),
                "resolution": info.get('resolution', 'unknown'),
                "width": info.get('width', 0),
                "height": info.get('height', 0),
                "fps": info.get('fps', 0),
                "video_codec": info.get('vcodec', 'unknown'),
                "audio_codec": info.get('acodec', 'unknown'),
                "download_timestamp": int(time.time())
            }
        
        # Add audio info if present
        if 'audio' in cloud_urls:
            response["audio"] = {
                "audio_url": cloud_urls['audio'],
                "format": audio_options.get('format', 'mp3'),
                "quality": audio_options.get('quality', '192')
            }
            
            # If there's only audio (no video), put basic info in media section too
            if 'video' not in cloud_urls:
                response["media"] = {
                    "media_url": cloud_urls['audio'],
                    "title": info.get('title', 'Unknown Title'),
                    "format_id": "audio_only",
                    "ext": audio_options.get('format', 'mp3'),
                    "download_timestamp": int(time.time())
                }
        
        return response, "/v1/media/download", 200
    
    except Exception as e:
        logger.error(f"Error processing download: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Make the error message more helpful
        error_message = f"Media download failed: {str(e)}"
        
        # Show details about the current state
        try:
            error_details = {
                "exception_type": type(e).__name__,
                "temp_directory": temp_dir,
                "media_url": media_url
            }
            
            if 'downloaded_files' in locals() and downloaded_files:
                error_details["downloaded_files"] = downloaded_files
                
            logger.error(f"Error details: {json.dumps(error_details)}")
        except:
            pass
        
        return error_message, "/v1/media/download", 500
    
    finally:
        # Always clean up the temporary directory
        try:
            if os.path.exists(temp_dir):
                logger.info(f"Cleaning up temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as cleanup_error:
            logger.error(f"Failed to clean up temporary directory: {str(cleanup_error)}")
